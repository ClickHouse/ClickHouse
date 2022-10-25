import pytest
import time
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/zookeeper_config.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def get_part_number(table_name):
    return TSV(
        node.query(
            f"SELECT count(*) FROM system.parts where table='{table_name}' and active=1"
        )
    )


def check_expected_part_number(seconds, table_name, expected):
    ok = False
    for i in range(int(seconds) * 2):
        result = get_part_number(table_name)
        if result == expected:
            ok = True
            break
        else:
            time.sleep(1)
    assert ok


def test_without_force_merge_old_parts(started_cluster):
    node.query(
        "CREATE TABLE test_without_merge (i Int64) ENGINE = MergeTree ORDER BY i;"
    )
    node.query("INSERT INTO test_without_merge SELECT 1")
    node.query("INSERT INTO test_without_merge SELECT 2")
    node.query("INSERT INTO test_without_merge SELECT 3")

    expected = TSV("""3\n""")
    # verify that the parts don't get merged
    for i in range(10):
        if get_part_number("test_without_merge") != expected:
            assert False
        time.sleep(1)

    node.query("DROP TABLE test_without_merge;")


@pytest.mark.parametrize("partition_only", ["True", "False"])
def test_force_merge_old_parts(started_cluster, partition_only):
    node.query(
        "CREATE TABLE test_with_merge (i Int64) "
        "ENGINE = MergeTree "
        "ORDER BY i "
        f"SETTINGS min_age_to_force_merge_seconds=5, min_age_to_force_merge_on_partition_only={partition_only};"
    )
    node.query("INSERT INTO test_with_merge SELECT 1")
    node.query("INSERT INTO test_with_merge SELECT 2")
    node.query("INSERT INTO test_with_merge SELECT 3")
    assert get_part_number("test_with_merge") == TSV("""3\n""")

    expected = TSV("""1\n""")
    check_expected_part_number(10, "test_with_merge", expected)

    node.query("DROP TABLE test_with_merge;")


@pytest.mark.parametrize("partition_only", ["True", "False"])
def test_force_merge_old_parts_replicated_merge_tree(started_cluster, partition_only):
    node.query(
        "CREATE TABLE test_replicated (i Int64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/testing/test', 'node') "
        "ORDER BY i "
        f"SETTINGS min_age_to_force_merge_seconds=5, min_age_to_force_merge_on_partition_only={partition_only};"
    )
    node.query("INSERT INTO test_replicated SELECT 1")
    node.query("INSERT INTO test_replicated SELECT 2")
    node.query("INSERT INTO test_replicated SELECT 3")
    assert get_part_number("test_replicated") == TSV("""3\n""")

    expected = TSV("""1\n""")
    check_expected_part_number(10, "test_replicated", expected)

    node.query("DROP TABLE test_replicated SYNC;")
