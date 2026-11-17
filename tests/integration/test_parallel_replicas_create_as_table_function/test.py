import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_reloaded_table_is_read_like_created_one(started_cluster):
    path = f"/var/lib/clickhouse/user_files/t_{uuid.uuid4().hex}/"
    node.query("DROP DATABASE IF EXISTS db SYNC")
    node.query("CREATE DATABASE db ENGINE = Atomic")
    node.query(
        f"CREATE TABLE db.t (id Int64) ENGINE = IcebergLocal('{path}', 'Parquet')"
    )
    node.query("INSERT INTO db.t SETTINGS allow_insert_into_iceberg = 1 VALUES (100)")

    # The profile reads an ad-hoc call of the table function through the cluster.
    assert "ReadFromCluster" in node.query(
        f"EXPLAIN SELECT sum(id) FROM icebergLocal('{path}', 'Parquet')"
    )

    node.query(f"CREATE TABLE db.tf (id Int64) AS icebergLocal('{path}', 'Parquet')")
    plan = node.query("EXPLAIN SELECT sum(id) FROM db.tf")
    node.query("DETACH DATABASE db")
    node.query("ATTACH DATABASE db")

    # A reloaded table is read the way the created one was.
    assert node.query("EXPLAIN SELECT sum(id) FROM db.tf") == plan
    assert node.query("SELECT sum(id) FROM db.tf") == "100\n"
    assert (
        node.query(
            "SELECT * FROM ((SELECT 'a', sum(id) FROM db.tf) UNION ALL (SELECT 'b', sum(id) FROM db.tf)) ORDER BY 1"
        )
        == "a\t100\nb\t100\n"
    )
    node.query("DROP DATABASE db SYNC")
