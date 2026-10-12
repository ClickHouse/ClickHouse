#!/usr/bin/env python3
# Tags: no-fasttest
# no-fasttest: requires clickhouse_driver

# A user without the `SHOW CONNECTIONS` privilege sees only the connections of this user in `system.connections`.
# The server config must set `collect_connection_metrics` to `true`.

import os
import urllib.parse
import urllib.request

from clickhouse_driver import Client

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

USER = f"user_{CLICKHOUSE_DATABASE}"


def query(sql, user="default"):
    url = HTTP_URL + "?" + urllib.parse.urlencode({"user": user})
    with urllib.request.urlopen(url, data=sql.encode(), timeout=60) as response:
        return response.read().decode().rstrip("\n")


query(f"DROP USER IF EXISTS {USER}")
query(f"CREATE USER {USER} NOT IDENTIFIED")
query(f"GRANT SELECT ON system.connections TO {USER}")

try:
    # A connection of a different user. This connection stays open and idle until the checks end.
    client = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, user="default", password="")
    other_id = client.execute("SELECT connection_id FROM system.connections WHERE query_id = currentQueryID()")[0][0]

    print("--- without SHOW CONNECTIONS: only the connections of this user")
    print(query(f"SELECT count() FROM system.connections WHERE connection_id = {other_id}", USER))
    print(query("SELECT countIf(user = currentUser()) > 0, countIf(user != currentUser()) FROM system.connections", USER))

    query(f"GRANT SHOW CONNECTIONS ON *.* TO {USER}")

    print("--- with SHOW CONNECTIONS: connections of all users")
    print(query(f"SELECT user FROM system.connections WHERE connection_id = {other_id}", USER))

    client.disconnect()
finally:
    query(f"DROP USER IF EXISTS {USER}")
