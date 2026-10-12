import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
nodes = [
    cluster.add_instance(f"node{num}", main_configs=["configs/remote_servers.xml"])
    for num in range(3)
]

ROWS = 512 * 512
SETTINGS = {
    "enable_parallel_replicas": 2,
    "max_parallel_replicas": 3,
    "cluster_for_parallel_replicas": "parallel_replicas",
    "parallel_replicas_for_non_replicated_merge_tree": 1,
    "parallel_replicas_local_plan": 0,
    "parallel_replicas_mark_segment_size": 128,
    "merge_tree_min_read_task_size": 1,
    "merge_tree_min_rows_for_concurrent_read": 0,
    "merge_tree_min_bytes_for_concurrent_read": 0,
    "max_threads": 1,
}


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Every node has a part `all_1_1_0` of a plain `MergeTree`, written with per-node settings and
# values. With `index_granularity` 512 and 513 the part has 512 marks either way, but the marks
# start at different rows. Adaptive granularity over fixed-size rows gives the same marks whether
# it is stored compressed or not.
FIXED_512 = "index_granularity = 512, index_granularity_bytes = 0"
FIXED_513 = "index_granularity = 513, index_granularity_bytes = 0"
ADAPTIVE = "index_granularity = 512, enable_index_granularity_compression = 1"
ADAPTIVE_UNCOMPRESSED = "index_granularity = 512, enable_index_granularity_compression = 0"
SAME_ROWS = ("number", "number", "number")


@pytest.mark.parametrize(
    "node_settings, node_values, refused",
    [
        ((FIXED_512, FIXED_512, FIXED_512), SAME_ROWS, False),
        ((FIXED_512, FIXED_513, FIXED_512), SAME_ROWS, True),
        ((ADAPTIVE, ADAPTIVE_UNCOMPRESSED, ADAPTIVE), SAME_ROWS, False),
        ((FIXED_512, FIXED_512, FIXED_512), ("number", "number + 1", "number"), True),
    ],
)
def test_same_named_parts(node_settings, node_values, refused):
    for node, settings, values in zip(nodes, node_settings, node_values):
        node.query("DROP TABLE IF EXISTS t SYNC")
        node.query(
            "CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a "
            f"SETTINGS min_bytes_for_wide_part = 0, {settings}"
        )
        node.query(f"INSERT INTO t SELECT {values} FROM numbers({ROWS})")
    parts = {
        node.query("SELECT name, marks FROM system.parts WHERE table = 't' AND active")
        for node in nodes
    }
    assert len(parts) == 1
    (part,) = parts
    assert part.startswith("all_1_1_0\t") and part.count("\n") == 1

    # Small read tasks and a slow read keep ranges being assigned until every replica has announced
    # its part.
    query = "SELECT count(), uniqExact(a) FROM t WHERE NOT ignore(sleepEachRow(0.00002))"
    query_ids = []
    for _ in range(3):
        query_id = str(uuid.uuid4())
        answer, error = nodes[0].query_and_get_answer_with_error(
            query, settings=SETTINGS, query_id=query_id
        )
        if refused:
            assert "BAD_ARGUMENTS" in error
        else:
            assert not error and answer == f"{ROWS}\t{ROWS}\n"
            query_ids.append(query_id)

    if query_ids:
        # Every replica got a read task, so every replica's announcement was compared.
        nodes[0].query("SYSTEM FLUSH LOGS")
        used = nodes[0].query(
            "SELECT groupArray(ProfileEvents['ParallelReplicasUsedCount']) FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND query_id IN {tuple(query_ids)}"
        )
        assert used == "[3,3,3]\n"
