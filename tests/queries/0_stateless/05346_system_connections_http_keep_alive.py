#!/usr/bin/env python3

# An HTTP keep-alive connection in `system.connections`:
# 1. The connection gets its registration on its first query. A `/ping` request does not register it.
# 2. All requests on the connection have the same `connection_id`.
# 3. The connection is idle between the requests.
# 4. Each request sets the client properties again, for example `client_name`.
# 5. The table does not show the connection after the socket closes.
# The server config must set `collect_connection_metrics` to `true`.

import http.client
import os
import time
import urllib.parse
import urllib.request

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

TIMEOUT = 60
USER_AGENT = "system-connections-keep-alive-test"
OTHER_USER_AGENT = "system-connections-keep-alive-test-2"


def observe(query):
    """Run a query on a new HTTP connection without keep-alive. Return the TSV output."""
    url = HTTP_URL + "?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response:
        return response.read().decode().rstrip("\n")


def wait_for(query, expected):
    deadline = time.monotonic() + TIMEOUT
    while True:
        result = observe(query)
        if result == expected:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"{query!r} returned {result!r}, expected {expected!r}")
        time.sleep(0.1)


connection = http.client.HTTPConnection(CLICKHOUSE_HOST, CLICKHOUSE_PORT_HTTP, timeout=TIMEOUT)


def request(method, path, body=None, user_agent=USER_AGENT):
    connection.request(method, path, body=body, headers={"User-Agent": user_agent})
    response = connection.getresponse()
    data = response.read().decode().rstrip("\n")
    assert response.status == 200, (response.status, data)
    assert not response.will_close, "the server closed the keep-alive connection"
    return data


def query(sql, user_agent=USER_AGENT):
    return request("POST", "/", sql, user_agent)


def idle_state(connection_id):
    # The server sends the response before it sets the connection to idle. Thus, wait for the idle status.
    wait_for(f"SELECT status FROM system.connections WHERE connection_id = {connection_id}", "idle")
    return observe(
        "SELECT protocol, status, query_id = '', user, client_name, last_query_time >= connected_time "
        f"FROM system.connections WHERE connection_id = {connection_id}"
    )


# When the keep-alive connection is open, no other connection has this client port.
request("GET", "/ping")
client_port = connection.sock.getsockname()[1]
by_port = f"protocol = 'HTTP' AND client_port = {client_port}"

print("--- not in the table before the first query")
print(observe(f"SELECT count() FROM system.connections WHERE {by_port}"))

print("--- the same connection_id for all requests")
own_id_query = "SELECT connection_id FROM system.connections WHERE query_id = currentQueryID() AND status = 'active'"
connection_id = query(own_id_query)
print(query(own_id_query) == connection_id)
print(observe(f"SELECT connection_id FROM system.connections WHERE {by_port}") == connection_id)

print("--- idle between the requests")
print(idle_state(connection_id))

print("--- the next request sets the client properties again")
print(query(own_id_query, OTHER_USER_AGENT) == connection_id)
print(idle_state(connection_id))

print("--- removed after the socket closes")
connection.close()
wait_for(f"SELECT count() FROM system.connections WHERE connection_id = {connection_id}", "0")
print("removed")
