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


def do_select(result, replica_num: int, read_count: int):
    data = (
        nodes[replica_num]
        .query(
            f"SELECT _queue_block_number FROM streaming_test STREAM LIMIT {read_count} FORMAT TabSeparated SETTINGS allow_experimental_streaming=1"
        )
        .split()
    )

    result[replica_num] = list([int(num) for num in data])


def do_insert(replica_num: int, count: int):
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

    results = [None, None]

    select_threads = [
        threading.Thread(target=do_select, args=(results, 0, T * N)),
        threading.Thread(target=do_select, args=(results, 1, T * N)),
    ]

    insert_threads = [
        threading.Thread(target=do_insert, args=(0, N)),
        threading.Thread(target=do_insert, args=(1, N)),
        threading.Thread(target=do_insert, args=(2, N)),
    ]

    # spawn streaming read from replica-0 before inserts
    select_threads[0].start()

    # spawn inserts
    for thread in insert_threads:
        thread.start()

    # spawn streaming read after inserts
    select_threads[1].start()

    for thread in select_threads:
        thread.join()

    for thread in insert_threads:
        thread.join()

    for res in results:
        assert len(res) == N * T
        assert sorted(res) == res
        assert res == [i for i in range(T * N)]

    assert results[0] == results[1]
