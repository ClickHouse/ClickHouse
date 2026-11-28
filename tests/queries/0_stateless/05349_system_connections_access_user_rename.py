#!/usr/bin/env python3
# Tags: no-fasttest
# no-fasttest: requires clickhouse_driver

# The `SHOW CONNECTIONS` check in `system.connections` uses the user ID, not the user name.
# A user keeps an open TCP connection and an idle HTTP keep-alive connection. Then an admin renames the user
# and creates a different user with the old name. The new user must not see the connections.
# The renamed user must still see them.
# The server config must set `collect_connection_metrics` to `true`.

import http.client
import os
import urllib.parse
import urllib.request

from clickhouse_driver import Client

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

OLD_NAME = f"alice_{CLICKHOUSE_DATABASE}"
NEW_NAME = f"bob_{CLICKHOUSE_DATABASE}"
OWN_ID_QUERY = "SELECT connection_id FROM system.connections WHERE query_id = currentQueryID()"


def query(sql, user="default"):
    url = HTTP_URL + "?" + urllib.parse.urlencode({"user": user})
    with urllib.request.urlopen(url, data=sql.encode(), timeout=60) as response:
        return response.read().decode().rstrip("\n")


def visible(user, ids):
    id_list = ", ".join(f"'{i}'" for i in ids)
    return query(f"SELECT count() FROM system.connections WHERE connection_id IN ({id_list})", user)


def create_user(name):
    query(f"CREATE USER {name} NOT IDENTIFIED")
    query(f"GRANT SELECT ON system.connections TO {name}")


for name in (OLD_NAME, NEW_NAME):
    query(f"DROP USER IF EXISTS {name}")
create_user(OLD_NAME)

try:
    # A persistent TCP connection of the first user.
    tcp_client = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, user=OLD_NAME, password="")
    tcp_id = str(tcp_client.execute(OWN_ID_QUERY)[0][0])

    # An HTTP keep-alive connection of the first user. It stays idle after this request.
    http_connection = http.client.HTTPConnection(CLICKHOUSE_HOST, CLICKHOUSE_PORT_HTTP, timeout=60)
    http_connection.request("POST", "/?" + urllib.parse.urlencode({"user": OLD_NAME}), body=OWN_ID_QUERY)
    response = http_connection.getresponse()
    http_id = response.read().decode().strip()
    assert response.status == 200, (response.status, http_id)
    assert not response.will_close, "the server closed the keep-alive connection"

    ids = [tcp_id, http_id]
    print("--- before the rename: the user sees both connections")
    print(visible(OLD_NAME, ids))

    query(f"ALTER USER {OLD_NAME} RENAME TO {NEW_NAME}")
    create_user(OLD_NAME)

    print("--- a new user with the old name does not see the connections")
    print(visible(OLD_NAME, ids))

    print("--- the renamed user still sees the connections")
    print(visible(NEW_NAME, ids))

    tcp_client.disconnect()
    http_connection.close()
finally:
    for name in (OLD_NAME, NEW_NAME):
        query(f"DROP USER IF EXISTS {name}")
