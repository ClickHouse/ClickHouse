import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_recovery_time_metric(start_cluster):
    node.query(
        """
        DROP DATABASE IF EXISTS rdb;
        CREATE DATABASE rdb
        ENGINE = Replicated('/test/test_recovery_time_metric', 'shard1', 'replica1')
        """
    )

    node.query(
        """
        DROP TABLE IF EXISTS rdb.t;
        CREATE TABLE rdb.t
        (
            `x` UInt32
        )
        ENGINE = MergeTree
        ORDER BY x
        """
    )

    db_disk_name = get_database_disk_name(node)
    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='rdb' AND name='t'"
    ).strip()

    node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "-C",
            "/etc/clickhouse-server/config.xml",
            "--disk",
            f"{db_disk_name}",
            "--save-logs",
            "--query",
            f"remove {metadata_path}",
        ],
        user="root",
    )

    node.restart_clickhouse()

    ret = int(
        node.query(
            """
            SELECT recovery_time
            FROM system.clusters
            WHERE cluster = 'rdb'
            """
        ).strip()
    )
    assert ret > 0

    node.query(
        """
        DROP DATABASE rdb
        """
    )
