from collections.abc import Iterable
import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster() -> Iterable[ClickHouseCluster]:
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("storage_policy", ["s3", "encrypted"])
def test_unfreeze(storage_policy: str, started_cluster: ClickHouseCluster) -> None:
    node1: ClickHouseInstance = started_cluster.instances["node1"]
    node1.query(
        f"""\
CREATE TABLE test1 (a Int)
ENGINE = ReplicatedMergeTree('/clickhouse-tables/test1', 'r1')
ORDER BY a
SETTINGS storage_policy = '{storage_policy}'
"""
    )

    node1.query(
        """\
INSERT INTO test1
SELECT *
FROM system.numbers
LIMIT 20
"""
    )

    node1.query("ALTER TABLE test1 FREEZE WITH NAME 'test'")
    node1.query("SYSTEM UNFREEZE WITH NAME 'test'")
    uuid = node1.query("SELECT uuid FROM system.tables WHERE name = 'test1'").strip()
    # ensure that zero copy lock parent still exists
    kazoo = started_cluster.get_kazoo_client("zoo1")
    part_path = f"/clickhouse/zero_copy/zero_copy_s3/{uuid}/all_0_0_0/"
    children: list[str] = kazoo.get_children(part_path)
    assert len(children) == 1
    part_name = children[0]
    assert len(kazoo.get_children(part_path + part_name)) == 1
    assert node1.query("SELECT count() FROM test1").strip() == "20"
    node1.query("DROP TABLE test1")
    node1.query("SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse-tables/test1'")
