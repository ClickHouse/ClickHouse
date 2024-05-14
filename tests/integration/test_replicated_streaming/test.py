import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        "node1",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 1},
    ),
    cluster.add_instance(
        "node2",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 2},
    ),
    cluster.add_instance(
        "node3",
        main_configs=["configs/clusters.xml"],
        with_zookeeper=True,
        macros={"shard": 1, "replica": 3},
    ),
]


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def do_inserts(replica_num: int, count: int):
    settings = ",".join(
        [
            "min_insert_block_size_rows=1",
            "max_block_size=1",
            "insert_keeper_fault_injection_probability=0",
        ]
    )

    start_num = replica_num * count

    nodes[replica_num].query(
        f"INSERT INTO streaming_test (*) select {start_num} + number, {start_num} + number from numbers({count}) SETTINGS {settings}"
    )


def test_streaming_read():
    nodes[0].query(
        "CREATE TABLE streaming_test ON CLUSTER 'cluster' (a UInt64, b UInt64) ENGINE=ReplicatedMergeTree() ORDER BY a SETTINGS queue_mode=1"
    )

    N = 1000
    T = 3

    insert_threads = [
        threading.Thread(target=do_inserts, args=(0, N)),
        threading.Thread(target=do_inserts, args=(1, N)),
        threading.Thread(target=do_inserts, args=(2, N)),
    ]

    for thread in insert_threads:
        thread.start()

    data = (
        nodes[0]
        .query(
            f"SELECT _queue_block_number FROM streaming_test STREAM LIMIT {N * T} FORMAT TabSeparated SETTINGS allow_experimental_streaming=1"
        )
        .split()
    )

    for thread in insert_threads:
        thread.join()

    data_numbers = [int(num) for num in data]
    assert len(data_numbers) == N * T
    assert sorted(data_numbers) == data_numbers
    assert data_numbers == [i for i in range(T * N)]
