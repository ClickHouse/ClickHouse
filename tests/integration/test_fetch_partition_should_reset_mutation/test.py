import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/zookeeper_config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_part_should_reset_mutation(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test', 'node') ORDER BY i;"
    )
    node.query("INSERT INTO test SELECT 1, 'a'")
    node.query("optimize table test final")
    node.query("optimize table test final")

    expected = TSV("""all_0_0_2\t1\ta""")
    assert TSV(node.query("SELECT _part, * FROM test")) == expected

    node.query(
        "ALTER TABLE test UPDATE s='xxx' WHERE 1", settings={"mutations_sync": "2"}
    )
    node.query(
        "ALTER TABLE test UPDATE s='xxx' WHERE 1", settings={"mutations_sync": "2"}
    )
    node.query(
        "ALTER TABLE test UPDATE s='xxx' WHERE 1", settings={"mutations_sync": "2"}
    )
    node.query(
        "ALTER TABLE test UPDATE s='xxx' WHERE 1", settings={"mutations_sync": "2"}
    )

    expected = TSV("""all_0_0_2_4\t1\txxx""")
    assert TSV(node.query("SELECT _part, * FROM test")) == expected

    node.query(
        "CREATE TABLE restore (i Int64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/restore', 'node') ORDER BY i;"
    )
    node.query(
        "ALTER TABLE restore FETCH PARTITION tuple() FROM '/clickhouse/tables/test/'"
    )
    node.query("ALTER TABLE restore ATTACH PART 'all_0_0_2_4'")
    node.query("INSERT INTO restore select 2, 'a'")

    print(TSV(node.query("SELECT _part, * FROM restore")))
    expected = TSV("""all_0_0_0\t1\txxx\nall_1_1_0\t2\ta""")
    assert TSV(node.query("SELECT _part, * FROM restore ORDER BY i")) == expected

    node.query(
        "ALTER TABLE restore UPDATE s='yyy' WHERE 1", settings={"mutations_sync": "2"}
    )

    expected = TSV("""all_0_0_0_2\t1\tyyy\nall_1_1_0_2\t2\tyyy""")
    assert TSV(node.query("SELECT _part, * FROM restore ORDER BY i")) == expected

    node.query("ALTER TABLE restore DELETE WHERE 1", settings={"mutations_sync": "2"})

    assert node.query("SELECT count() FROM restore").strip() == "0"
