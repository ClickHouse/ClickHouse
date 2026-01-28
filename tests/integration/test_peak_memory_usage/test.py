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
            "INSERT INTO fixed_numbers_2 SELECT number FROM numbers(0, 120000)"
        )

        yield cluster
    finally:
        cluster.shutdown()


def get_memory_usage_from_client_output_and_close(client_output):
    client_output.seek(0)
    query_id = None
    peak_memory_usage = None
    peek_memory_usage_str_found = False
    
    for line in client_output:
        print(f"'{line}'\n")
        
        # Extract query ID
        if query_id is None and "Query id:" in line:
            query_id_match = re.search(r"Query id: ([a-f0-9-]+)", line)
            if query_id_match:
                query_id = query_id_match.group(1)
                print(f"query_id {query_id}")
        
        # Extract peak memory usage
        if not peek_memory_usage_str_found:
            # Can be both Peak/peak
            peek_memory_usage_str_found = "eak memory usage" in line

        if peek_memory_usage_str_found and peak_memory_usage is None:
            search_obj = re.search(r"[+-]?[0-9]+\.[0-9]+", line)
            if search_obj:
                peak_memory_usage = search_obj.group()
                print(f"peak_memory_usage {peak_memory_usage}")
    
    client_output.close()
    
    if query_id is None:
        print(f"query_id not found")
    if peak_memory_usage is None:
        print(f"peak_memory_usage not found")
    
    return query_id, peak_memory_usage


def test_clickhouse_client_max_peak_memory_usage_distributed(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")
    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM distributed_fixed_numbers JOIN fixed_numbers_2 ON distributed_fixed_numbers.number=fixed_numbers_2.number SETTINGS query_plan_join_swap_table = 'false', join_algorithm='hash'",
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    query_id, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert query_id
    assert peak_memory_usage
    
    # Extract required_query_id from shard2 logs
    required_query_id = None
    shard_2_logs = shard_2.grep_in_log(f"initial_query_id: {query_id}")
    
    if shard_2_logs:
        log_lines = shard_2_logs.strip().split('\n')
        for log_line in log_lines:
            query_id_match = re.search(r"\{([a-f0-9-]+)\}", log_line)
            if query_id_match:
                required_query_id = query_id_match.group(1)
                break
    
    assert required_query_id, "Failed to extract required_query_id from shard2 logs"
    
    # Extract server peak memory usage from logs
    server_memory_log = shard_2.grep_in_log(f"{{{required_query_id}}} <Debug> MemoryTracker: Query peak memory usage:")
    assert server_memory_log, f"Memory usage log not found for query {required_query_id}"
    
    # Parse server memory usage value
    server_memory_match = re.search(r"Query peak memory usage: ([0-9.]+)", server_memory_log)
    assert server_memory_match, "Failed to parse server memory usage"
    server_memory_value = float(server_memory_match.group(1))
    
    # Parse client memory usage value
    client_memory_value = float(peak_memory_usage)
    
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
            "SELECT COUNT(*) FROM (SELECT number FROM numbers(1,300000) INTERSECT SELECT number FROM numbers(10000,1200000))"
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    query_id, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert query_id
    assert peak_memory_usage
    assert shard_1.contains_in_log(f"Query peak memory usage: {peak_memory_usage}")


    
