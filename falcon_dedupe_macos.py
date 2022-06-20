import os
import pandas as pd

from falconpy import Hosts

# These need set in your shell.
CLIENT_ID = os.environ["FALCON_CLIENT_ID"]
CLIENT_SECRET = os.environ["FALCON_CLIENT_SECRET"]


if __name__ == "__main__":
    # Retrieves all hosts that match the "MAC-" prefix.
    # Limits results to 5000 due to laziness around pagination.
    hosts = Hosts(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    resp = hosts.query_devices_by_filter(filter="hostname:'MAC-'", limit=5000)

    if resp["status_code"] == 200:
        hosts_ids = resp["body"]["resources"]
        matched_hosts = hosts.get_device_details(ids=hosts_ids)["body"]["resources"]

        # Adds any matched hosts to the pandas DataFrame.
        # We use pandas because it lets us sort and identify duplicates easily.
        # Ref: https://www.statology.org/pandas-add-row-to-dataframe
        df = pd.DataFrame(columns=["device_id", "hostname", "last_seen"])
        for host in matched_hosts:
            df.loc[len(df.index)] = [
                host["device_id"],
                host["hostname"],
                host["last_seen"],
            ]

        # Identifies duplicates and sorts based on the "last_seen" date.
        # The `.head(-1)` piece is pretty important, as it will include all duplicates before the last row (eg: the working agent).
        # Ref: https://stackoverflow.com/questions/57550884/pandas-how-to-keep-the-last-n-records-of-each-group-sorted-by-another-variabl
        duplicates = df.loc[df.duplicated(["hostname"], keep=False)]
        sorted_duplicates = (
            duplicates.sort_values(["last_seen"]).groupby("hostname").head(-1)
        )

        # Hacky audit logging
        for row in sorted_duplicates.iterrows():
            print(
                "Removing duplicate %s (%s) with last check-in on %s"
                % (row[1]["hostname"], row[1]["device_id"], row[1]["last_seen"])
            )

        # Only stale agents are in `sorted_duplicates` so no issue with removing them from Falcon.
        # This is not documented in the API, but the `ids` parameter is limited to 100.
        # Ref: https://www.falconpy.io/Service-Collections/Hosts.html#performactionv2
        # Ref: https://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks
        chunk_size = 100
        for i in range(0, len(sorted_duplicates["device_id"].tolist()), chunk_size):
            chunk = sorted_duplicates["device_id"].tolist()[i : i + chunk_size]

            """
            resp = hosts.perform_action(action_name="hide_host", ids=chunk)
            if resp["status_code"] != 200:
                for error in resp["body"]["errors"]:
                    error_code = error["code"]
                    error_message = error["message"]
                    print(f"[Error {error_code}] {error_message}")
            """

    else:
        for error in resp["body"]["errors"]:
            error_code = error["code"]
            error_message = error["message"]
            print(f"[Error {error_code}] {error_message}")
