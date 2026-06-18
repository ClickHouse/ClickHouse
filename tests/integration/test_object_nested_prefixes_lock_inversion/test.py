import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/prefixes_pool.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Reading a JSON column whose values nest an Array(JSON) inside another Array(JSON)
# makes prefix deserialization recurse into nested `SerializationObject`s while it is
# parallelized over the prefixes deserialization thread pool. Each nesting level used
# to create its own stack `callbacks_mutex`; with warm pool threads (see the config)
# the levels land on different threads at the same stack frame offset, and TSan keys
# mutexes by address and keeps lock-order edges past destruction, so reused stack
# addresses formed a phantom `M0 => M1 => M0` cycle reported as `lock-order-inversion`.
# Under the thread sanitizer that aborts the server; here we just read the table many
# times and check it stays alive. See google/sanitizers#1717.
def test_nested_json_prefixes_no_lock_inversion(start_cluster):
    node.query("DROP TABLE IF EXISTS t_object_nested_prefixes SYNC")
    node.query(
        """
        CREATE TABLE t_object_nested_prefixes (c JSON)
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        """,
        settings={"allow_experimental_json_type": 1},
    )
    # Many small wide parts, each with three nested Object levels (top object, `arr`
    # and `arr2`), so a single read fans many nested prefix tasks into the pool.
    node.query("SYSTEM STOP MERGES t_object_nested_prefixes")
    insert = """
        INSERT INTO t_object_nested_prefixes
        SELECT concat('{',
            arrayStringConcat(arrayMap(i -> concat('"a', toString(i), '":', toString(number + i)), range(40)), ','),
            ',"arr":[{', arrayStringConcat(arrayMap(i -> concat('"b', toString(i), '":', toString(number + i)), range(40)), ','),
            ',"arr2":[{', arrayStringConcat(arrayMap(i -> concat('"d', toString(i), '":', toString(number + i)), range(40)), ','),
            '}]}]}')::JSON
        FROM numbers(5)
    """
    for _ in range(50):
        node.query(insert, settings={"allow_experimental_json_type": 1})

    # Low max_threads keeps few concurrent outer tasks, leaving warm pool threads idle
    # to pick up the nested tasks directly (instead of being work-stolen by the parent).
    for _ in range(20):
        node.query(
            "SELECT count() FROM t_object_nested_prefixes WHERE NOT ignore(c)",
            settings={
                "merge_tree_use_prefixes_deserialization_thread_pool": 1,
                "max_threads": 2,
                "allow_experimental_json_type": 1,
            },
        )

    assert node.query("SELECT count() FROM t_object_nested_prefixes") == "250\n"

    node.query("DROP TABLE t_object_nested_prefixes SYNC")
