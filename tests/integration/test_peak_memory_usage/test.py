import re
import tempfile

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.uclient import client, prompt

cluster = ClickHouseCluster(__file__)

shard_1 = cluster.add_instance(
    "shard_1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={
        "shard": "shard_1",
    },
)
shard_2 = cluster.add_instance(
    "shard_2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={
        "shard": "shard_2",
    },
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        shard_1.query(
            "CREATE TABLE fixed_numbers ON CLUSTER 'cluster' ("
            "number UInt64"
            ") ENGINE=MergeTree()"
            "ORDER BY number"
        )

        shard_1.query(
            "CREATE TABLE fixed_numbers_2 ON CLUSTER 'cluster' ("
            "number UInt64"
            ") ENGINE=Memory ()"
        )

        shard_1.query(
            "CREATE TABLE distributed_fixed_numbers (number UInt64) ENGINE=Distributed('cluster', 'default', 'fixed_numbers')"
        )
        shard_1.query("INSERT INTO fixed_numbers SELECT number FROM numbers(0, 100)")

        shard_2.query("INSERT INTO fixed_numbers SELECT number FROM numbers(100, 200)")

        shard_1.query("INSERT INTO fixed_numbers_2 SELECT number FROM numbers(0, 10)")

        shard_2.query(
            "INSERT INTO fixed_numbers_2 SELECT number FROM numbers(0, 1200000)"
        )

        yield cluster
    finally:
        cluster.shutdown()


def get_memory_usage_from_client_output_and_close(client_output, shard_name):
    client_output.seek(0)
    client_memory_value = None
    server_memory_value = None
    peak_memory_usage_found = False

    for line in client_output:
        # Peak memory usage should be on the specified shard
        if server_memory_value is None and shard_name in line:
            server_match = re.search(r"Query peak memory usage: ([0-9.]+)", line)
            if server_match:
                server_memory_value = float(server_match.group(1))
                print(f"Server peak memory usage: {server_memory_value}")
        
        # Extract client peak memory usage (appears after "client1>Peak memory usage" line)
        if "client1>Peak memory usage" in line and client_memory_value is None:
            peak_memory_usage_found = True
        elif peak_memory_usage_found and client_memory_value is None and "client1>:" in line:
            # Value is on this line (e.g., "client1>: 160.26 MiB.")
            search_obj = re.search(r"[+-]?[0-9]+\.[0-9]+", line)
            if search_obj:
                client_memory_value = float(search_obj.group())
                print(f"Client peak memory usage: {client_memory_value}")
                peak_memory_usage_found = False
    
    client_output.close()
    
    assert client_memory_value is not None, "Client peak memory usage not found"
    assert server_memory_value is not None, "Server peak memory usage not found"
    
    return client_memory_value, server_memory_value


def test_clickhouse_client_max_peak_memory_usage_distributed(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")
    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM distributed_fixed_numbers JOIN fixed_numbers_2 ON distributed_fixed_numbers.number=fixed_numbers_2.number SETTINGS query_plan_join_swap_table = 'false', join_algorithm='hash', send_logs_level='trace', send_logs_source_regexp='MemoryTracker'",
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    client_memory_value, server_memory_value = get_memory_usage_from_client_output_and_close(client_output, "shard_2")
    
    # Assert difference is less than 1 MB
    memory_diff = abs(client_memory_value - server_memory_value)
    assert memory_diff < 1.0, f"Memory usage difference {memory_diff} MiB exceeds 1 MiB threshold (client: {client_memory_value} MiB, server: {server_memory_value} MiB)"


def test_clickhouse_client_max_peak_memory_single_node(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")

    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM (SELECT number FROM numbers(1,300000) INTERSECT SELECT number FROM numbers(10000,1200000)) SETTINGS send_logs_level='trace', send_logs_source_regexp='MemoryTracker'"
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    client_memory_value, server_memory_value = get_memory_usage_from_client_output_and_close(client_output, "shard_1")
    
    # Assert difference is less than 1 MB
    memory_diff = abs(client_memory_value - server_memory_value)
    assert memory_diff < 1.0, f"Memory usage difference {memory_diff} MiB exceeds 1 MiB threshold (client: {client_memory_value} MiB, server: {server_memory_value} MiB)"


    
