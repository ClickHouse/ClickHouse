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
    peek_memory_usage_str_found = False
    for line in client_output:
        print(f"'{line}'\n")
        if not peek_memory_usage_str_found:
            # Can be both Peak/peak
            peek_memory_usage_str_found = "eak memory usage" in line

        if peek_memory_usage_str_found:
            search_obj = re.search(r"[+-]?[0-9]+\.[0-9]+", line)
            if search_obj:
                client_output.close()
                print(f"peak_memory_usage {search_obj.group()}")
                return search_obj.group()

    print(f"peak_memory_usage not found")
    client_output.close()
    return ""


def test_clickhouse_client_max_peak_memory_usage_distributed(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")
    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM distributed_fixed_numbers JOIN fixed_numbers_2 ON distributed_fixed_numbers.number=fixed_numbers_2.number SETTINGS query_plan_join_inner_table_selection = 'right'",
        )
        client1.expect("Peak memory usage", timeout=60)
        client1.expect(prompt)

    peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_2.contains_in_log(f"Query peak memory usage: {peak_memory_usage}")


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

    peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_1.contains_in_log(f"Query peak memory usage: {peak_memory_usage}")
