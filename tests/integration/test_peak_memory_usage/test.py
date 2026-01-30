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
            "SELECT COUNT(*) FROM distributed_fixed_numbers JOIN fixed_numbers_2 ON distributed_fixed_numbers.number=fixed_numbers_2.number SETTINGS query_plan_join_swap_table = 'false', join_algorithm='hash', send_logs_level='trace', send_logs_source_regexp='MemoryTracker'",
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    query_id, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert query_id
    assert peak_memory_usage
    
    # Extract required_query_id from shard2 logs
    print(f"Searching shard2 logs for initial_query_id: {query_id}")
    required_query_id = None
    shard_2_logs = shard_2.grep_in_log(f"initial_query_id: {query_id}")
    print(f"Shard2 logs result:\n{shard_2_logs}")
    
    if shard_2_logs:
        log_lines = shard_2_logs.strip().split('\n')
        print(f"Found {len(log_lines)} log lines with initial_query_id")
        for log_line in log_lines:
            print(f"Processing log line: {log_line}")
            # Extract query_id from curly braces {required_query_id}
            query_id_match = re.search(r"\{([a-f0-9-]+)\}", log_line)
            if query_id_match:
                required_query_id = query_id_match.group(1)
                print(f"required_query_id from shard2: {required_query_id}")
                break
    
    assert required_query_id, "Failed to extract required_query_id from shard2 logs"
    
    # Assert that shard2 logs contain the query peak memory usage with the required_query_id
    expected_log_pattern = f"{{{required_query_id}}} <Debug> MemoryTracker: Query peak memory usage: {peak_memory_usage}"
    print(f"Checking for log pattern: {expected_log_pattern}")
    assert shard_2.contains_in_log(expected_log_pattern)


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

    query_id, peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert query_id
    assert peak_memory_usage
    assert shard_1.contains_in_log(f"Query peak memory usage: {peak_memory_usage}")


    
