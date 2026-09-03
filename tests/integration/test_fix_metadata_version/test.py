import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    stay_alive=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_fix_metadata_version(start_cluster):
    zookeeper_path = "/clickhouse/test_fix_metadata_version"
    replica = "replica1"
    replica_path = f"{zookeeper_path}/replicas/{replica}"

    def get_metadata_versions():
        table_metadata_version = int(
            node.query(
                f"""
                SELECT version
                FROM system.zookeeper
                WHERE path = '{zookeeper_path}' AND name = 'metadata'
                """
            ).strip()
        )

        replica_metadata_version = int(
            node.query(
                f"""
                SELECT value
                FROM system.zookeeper
                WHERE path = '{replica_path}' AND name = 'metadata_version'
                """
            ).strip()
        )

        return table_metadata_version, replica_metadata_version

    node.query(
        f"""
        DROP TABLE IF EXISTS t SYNC;
        CREATE TABLE t
        (
            `x` UInt32
        )
        ENGINE = ReplicatedMergeTree('{zookeeper_path}', '{replica}')
        ORDER BY x
        """
    )

    node.query("ALTER TABLE t (ADD COLUMN `y` UInt32)")

    assert get_metadata_versions() == (1, 1)

    cluster.query_zookeeper(f"set '{replica_path}/metadata_version' '0'")

    assert get_metadata_versions() == (1, 0)

    node.restart_clickhouse()

    assert get_metadata_versions() == (1, 1)
