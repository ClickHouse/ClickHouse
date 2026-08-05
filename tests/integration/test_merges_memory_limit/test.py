import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_limit_success():
    if node.is_built_with_thread_sanitizer():
        pytest.skip(
            "tsan build is skipped because it slowly merges the parts, "
            "rather than failing over the memory limit"
        )

    node.query(
        "CREATE TABLE test_merge_oom ENGINE=AggregatingMergeTree ORDER BY id EMPTY AS SELECT number%1024 AS id, arrayReduce('groupArrayState', arrayMap(x-> randomPrintableASCII(100), range(8192))) fat_state FROM numbers(20000)"
    )
    node.query("SYSTEM STOP MERGES test_merge_oom")
    node.query(
        "INSERT INTO test_merge_oom SELECT number%1024 AS id, arrayReduce('groupArrayState', arrayMap( x-> randomPrintableASCII(100), range(8192))) fat_state FROM numbers(3000)"
    )
    node.query(
        "INSERT INTO test_merge_oom SELECT number%1024 AS id, arrayReduce('groupArrayState', arrayMap( x-> randomPrintableASCII(100), range(8192))) fat_state FROM numbers(3000)"
    )
    node.query(
        "INSERT INTO test_merge_oom SELECT number%1024 AS id, arrayReduce('groupArrayState', arrayMap( x-> randomPrintableASCII(100), range(8192))) fat_state FROM numbers(3000)"
    )

    _, error = node.query_and_get_answer_with_error(
        """
        SET optimize_throw_if_noop=1;
        SYSTEM START MERGES test_merge_oom;
        OPTIMIZE TABLE test_merge_oom FINAL;
        """
    )

    assert not error
    node.query("DROP TABLE test_merge_oom")
