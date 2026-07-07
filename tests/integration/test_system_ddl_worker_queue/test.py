import pytest
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node4 = cluster.add_instance(
    "node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)

nodes = [node1, node2, node3, node4]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def maintain_test_table(test_table):
    tmark = time.time()  # to guarantee ZK path uniqueness

    for i, node in enumerate([node1, node2]):
        node.query(f"DROP TABLE IF EXISTS testdb.{test_table} SYNC")
        node.query("DROP DATABASE IF EXISTS testdb")

        node.query("CREATE DATABASE testdb")
        node.query(
            f"CREATE TABLE testdb.{test_table}(id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/test/{test_table}1-{tmark}', '{i}') ORDER BY id;"
        )
    for i, node in enumerate([node3, node4]):
        node.query(f"DROP TABLE IF EXISTS testdb.{test_table} SYNC")
        node.query("DROP DATABASE IF EXISTS testdb")

        node.query("CREATE DATABASE testdb")
        node.query(
            f"CREATE TABLE testdb.{test_table}(id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/test/{test_table}2-{tmark}', '{i}') ORDER BY id;"
        )


def test_distributed_ddl_queue(started_cluster):
    test_table = "test_table"
    maintain_test_table(test_table)
    node1.query(
        f"INSERT INTO testdb.{test_table} SELECT number, toString(number) FROM numbers(100)"
    )
    node3.query(
        f"INSERT INTO testdb.{test_table} SELECT number, toString(number) FROM numbers(100)"
    )
    node2.query(f"SYSTEM SYNC REPLICA testdb.{test_table}")
    node4.query(f"SYSTEM SYNC REPLICA testdb.{test_table}")

    node1.query(
        f"ALTER TABLE testdb.{test_table} ON CLUSTER test_cluster ADD COLUMN somecolumn UInt8 AFTER val",
        settings={"replication_alter_partitions_sync": "2"},
    )
    for node in nodes:
        node.query(f"SYSTEM SYNC REPLICA testdb.{test_table}")
        assert (
            node.query(f"SELECT somecolumn FROM testdb.{test_table} LIMIT 1") == "0\n"
        )
        assert (
            node.query(
                "SELECT If((SELECT count(*) FROM system.distributed_ddl_queue  WHERE cluster='test_cluster' AND entry='query-0000000000') > 0, 'ok', 'fail')"
            )
            == "ok\n"
        )

    node1.query(
        f"ALTER TABLE testdb.{test_table} ON CLUSTER test_cluster DROP COLUMN somecolumn",
        settings={"replication_alter_partitions_sync": "2"},
    )


def test_distributed_ddl_rubbish(started_cluster):
    test_table = "test_table_rubbish"
    maintain_test_table(test_table)
    node1.query(
        f"ALTER TABLE testdb.{test_table} ON CLUSTER test_cluster ADD COLUMN somenewcolumn UInt8 AFTER val",
        settings={"replication_alter_partitions_sync": "2"},
    )

    zk_content = node1.query(
        "SELECT name, value, path FROM system.zookeeper WHERE path LIKE '/clickhouse/task_queue/ddl%' SETTINGS allow_unrestricted_reads_from_keeper=true",
        parse=True,
    ).to_dict("records")

    original_query = ""
    new_query = "query-artificial-" + str(time.monotonic_ns())

    # Copy information about query (one that added 'somenewcolumn') with new query ID
    # and broken query text (TABLE => TUBLE)
    for row in zk_content:
        if row["value"].find("somenewcolumn") >= 0:
            original_query = row["name"]
            break

    rows_to_insert = []

    for row in zk_content:
        if row["name"] == original_query:
            rows_to_insert.append(
                {
                    "name": new_query,
                    "path": row["path"],
                    "value": row["value"].replace("TABLE", "TUBLE"),
                }
            )
            continue
        pos = row["path"].find(original_query)
        if pos >= 0:
            rows_to_insert.append(
                {
                    "name": row["name"],
                    "path": row["path"].replace(original_query, new_query),
                    "value": row["value"],
                }
            )

    # Ingest it to ZK
    for row in rows_to_insert:
        node1.query(
            "insert into system.zookeeper (name, path, value) values ('{}', '{}', '{}')".format(
                f'{row["name"]}', f'{row["path"]}', f'{row["value"]}'
            )
        )

    # Ensure that data is visible via system.distributed_ddl_queue
    assert (
        int(
            node1.query(
                f"SELECT count(1) FROM system.distributed_ddl_queue WHERE entry='{new_query}' AND cluster=''"
            )
        )
        == 4
    )

    node1.query(
        f"ALTER TABLE testdb.{test_table} ON CLUSTER test_cluster DROP COLUMN somenewcolumn",
        settings={"replication_alter_partitions_sync": "2"},
    )
