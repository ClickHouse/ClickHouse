#!/usr/bin/env python3
# Tags: no-fasttest
# no-fasttest: requires clickhouse_driver

# Lifecycle of one persistent native TCP connection in `system.connections`:
# 1. The connection is idle after a query.
# 2. The connection is active when a query runs.
# 3. The connection is idle after a query fails or after KILL QUERY.
# 4. The table does not show the connection after the client disconnects.
# The test uses the same `connection_id` for all steps. Other connections read the state of this connection.
# The server config must set `collect_connection_metrics` to `true`.

import os
import time
import urllib.parse
import urllib.request
import uuid

from clickhouse_driver import Client
from clickhouse_driver.protocol import ServerPacketTypes

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT_TCP = int(os.environ.get("CLICKHOUSE_PORT_TCP", "9000"))
CLICKHOUSE_PORT_HTTP = int(os.environ.get("CLICKHOUSE_PORT_HTTP", "8123"))
HTTP_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT_HTTP}/"

TIMEOUT = 60


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


def send_query(connection, query, query_id):
    connection.send_query(query, query_id=query_id)
    connection.send_external_tables(None)


def receive_result(connection):
    """Read the result of a query. A server exception does not close the connection. `Client.execute` closes it."""
    while True:
        packet = connection.receive_packet()
        if packet.type == ServerPacketTypes.EXCEPTION:
            return packet.exception.code
        if packet.type == ServerPacketTypes.END_OF_STREAM:
            return 0


def idle_state(connection_id):
    # The server sends the end of the result before it sets the connection to idle. Thus, wait for the idle status.
    wait_for(f"SELECT status FROM system.connections WHERE connection_id = {connection_id}", "idle")
    return observe(
        "SELECT protocol, status, query_id = '', user, last_query_time >= connected_time "
        f"FROM system.connections WHERE connection_id = {connection_id}"
    )


client = Client(CLICKHOUSE_HOST, port=CLICKHOUSE_PORT_TCP, user="default", password="")

# The query sees its connection as active.
connection_id = client.execute(
    "SELECT connection_id FROM system.connections WHERE query_id = currentQueryID() AND status = 'active'"
)[0][0]
connection = client.connection

print("--- idle after a successful query")
print(idle_state(connection_id))

print("--- active when a query runs")
running_query_id = str(uuid.uuid4())
send_query(connection, "SELECT sleepEachRow(0.1) FROM system.numbers SETTINGS max_block_size = 1", running_query_id)
wait_for(
    f"SELECT status FROM system.connections WHERE connection_id = {connection_id} AND query_id = '{running_query_id}'",
    "active",
)
print("active")
observe(f"KILL QUERY WHERE query_id = '{running_query_id}' SYNC FORMAT Null")
receive_result(connection)

print("--- idle after KILL QUERY")
print(idle_state(connection_id))

print("--- idle after a query fails")
failed_query_id = str(uuid.uuid4())
send_query(connection, "SELECT throwIf(number = 3, 'intentional test exception') FROM numbers(10) SETTINGS max_block_size = 1", failed_query_id)
print(receive_result(connection))
print(idle_state(connection_id))
print(observe(f"SELECT count() FROM system.connections WHERE query_id = '{failed_query_id}'"))

print("--- the connection can run a query")
send_query(connection, "SELECT 1", str(uuid.uuid4()))
print(receive_result(connection))

print("--- removed after disconnect")
client.disconnect()
wait_for(f"SELECT count() FROM system.connections WHERE connection_id = {connection_id}", "0")
print("removed")
