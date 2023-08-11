import pytest
import tempfile
import re

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
            "CREATE TABLE distributed_fixed_numbers (number UInt64) ENGINE=Distributed('cluster', 'default', 'fixed_numbers')"
        )

        # Shard 1 has  singificantly less data then shard 2
        shard_1.query(
            "INSERT INTO fixed_numbers SELECT number FROM numbers(1999900, 2000000)"
        )

        shard_2.query(
            "INSERT INTO fixed_numbers SELECT number FROM numbers(0, 1999900)"
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
            peek_memory_usage_str_found = "Peak memory usage" in line

        if peek_memory_usage_str_found:
            search_obj = re.search(r"([-+]?(?:\d*\.*\d+))", line)
            if search_obj:
                client_output.close()
                return search_obj.group()

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
            "SELECT COUNT(*) FROM distributed_fixed_numbers WHERE number IN (SELECT number from numbers(1999890, 1999910))"
        )
        client1.expect("Peak memory usage")
        client1.expect(prompt)

    peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_2.contains_in_log(
        f"Peak memory usage (for query): {peak_memory_usage}"
    )

def test_clickhouse_client_max_peak_memory_usage_cluster(started_cluster):
    client_output = tempfile.TemporaryFile(mode="w+t")
    command_text = (
        f"{started_cluster.get_client_cmd()} --host {shard_1.ip_address} --port 9000"
    )
    with client(name="client1>", log=client_output, command=command_text) as client1:
        client1.expect(prompt)
        client1.send(
            "SELECT COUNT(*) FROM (SELECT number FROM numbers(1,100000) INTERSECT SELECT * FROM clusterAllReplicas(cluster, default, fixed_numbers))"
        )
        client1.expect("Peak memory usage")
        client1.expect(prompt)

    peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_2.contains_in_log(
        f"Peak memory usage (for query): {peak_memory_usage}"
    )


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
        client1.expect("Peak memory usage")
        client1.expect(prompt)

    peak_memory_usage = get_memory_usage_from_client_output_and_close(client_output)
    assert peak_memory_usage
    assert shard_1.contains_in_log(
        f"Peak memory usage (for query): {peak_memory_usage}"
    )
