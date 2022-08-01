import pytest
import time
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/zookeeper_config.xml", "configs/merge_tree.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def check_expected_result_or_fail(seconds, expected):
    ok = False
    for i in range(int(seconds) * 2):
        result = TSV(
            node.query(
                "SELECT count(*) FROM system.parts where table='test' and active=1"
            )
        )
        if result == expected:
            ok = True
            break
        else:
            time.sleep(0.5)
    assert(ok)

def test_without_auto_optimize_merge_tree(start_cluster):
    node.query("CREATE TABLE test (i Int64) ENGINE = MergeTree ORDER BY i;")
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")


    expected = TSV("""3\n""")
    check_expected_result_or_fail(5, expected)

    node.query("DROP TABLE test;")


def test_auto_optimize_merge_tree(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64) ENGINE = MergeTree ORDER BY i SETTINGS auto_optimize_partition_after_seconds=5;"
    )
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")

    expected = TSV("""1\n""")
    check_expected_result_or_fail(10, expected)

    node.query("DROP TABLE test;")


def test_auto_optimize_replicated_merge_tree(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/testing/test', 'node') ORDER BY i SETTINGS auto_optimize_partition_after_seconds=5;"
    )
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")

    expected = TSV("""1\n""")
    check_expected_result_or_fail(10, expected)

    node.query("DROP TABLE test;")
