import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for i, node in enumerate([node1, node2]):
            node.query("CREATE DATABASE testdb")
            node.query(
                """CREATE TABLE testdb.test_table(id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/test/test_table1', '{}') ORDER BY id;""".format(
                    i
                )
            )
        for i, node in enumerate([node3, node4]):
            node.query("CREATE DATABASE testdb")
            node.query(
                """CREATE TABLE testdb.test_table(id UInt32, val String) ENGINE = ReplicatedMergeTree('/clickhouse/test/test_table2', '{}') ORDER BY id;""".format(
                    i
                )
            )
        yield cluster

    finally:
        cluster.shutdown()


def test_alter(started_cluster):
    node1.query(
        "INSERT INTO testdb.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node3.query(
        "INSERT INTO testdb.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    node2.query("SYSTEM SYNC REPLICA testdb.test_table")
    node4.query("SYSTEM SYNC REPLICA testdb.test_table")

    node1.query(
        "ALTER TABLE testdb.test_table ON CLUSTER test_cluster ADD COLUMN somecolumn UInt8 AFTER val",
        settings={"replication_alter_partitions_sync": "2"},
    )

    node1.query("SYSTEM SYNC REPLICA testdb.test_table")
    node2.query("SYSTEM SYNC REPLICA testdb.test_table")
    node3.query("SYSTEM SYNC REPLICA testdb.test_table")
    node4.query("SYSTEM SYNC REPLICA testdb.test_table")

    assert node1.query("SELECT somecolumn FROM testdb.test_table LIMIT 1") == "0\n"
    assert node2.query("SELECT somecolumn FROM testdb.test_table LIMIT 1") == "0\n"
    assert node3.query("SELECT somecolumn FROM testdb.test_table LIMIT 1") == "0\n"
    assert node4.query("SELECT somecolumn FROM testdb.test_table LIMIT 1") == "0\n"


def test_ddl_queue_hostname_change(started_cluster):
    node1.query(
        "create table hostname_change on cluster test_cluster (n int) engine=Log"
    )

    # There's no easy way to change hostname of a container, so let's update values in zk
    query_znode = node1.query(
        "select max(name) from system.zookeeper where path='/clickhouse/task_queue/ddl'"
    )[:-1]

    value = (
        node1.query(
            "select value from system.zookeeper where path='/clickhouse/task_queue/ddl' and name='{}' format TSVRaw".format(
                query_znode
            )
        )[:-1]
        .replace("\\'", "#")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("#", "\\'")
    )

    finished_znode = node1.query(
        "select name from system.zookeeper where path='/clickhouse/task_queue/ddl/{}/finished' and name like '%node1%'".format(
            query_znode
        )
    )[:-1]

    node1.query(
        "insert into system.zookeeper (name, path, value) values ('{}', '/clickhouse/task_queue/ddl', '{}')".format(
            query_znode, value.replace("node1", "imaginary.old.hostname")
        )
    )
    started_cluster.get_kazoo_client("zoo1").delete(
        "/clickhouse/task_queue/ddl/{}/finished/{}".format(query_znode, finished_znode)
    )

    node1.query(
        "insert into system.zookeeper (name, path, value) values ('{}', '/clickhouse/task_queue/ddl/{}/finished', '0\\n')".format(
            finished_znode.replace("node1", "imaginary.old.hostname"), query_znode
        )
    )

    node1.restart_clickhouse(kill=True)

    node1.query(
        "create table hostname_change2 on cluster test_cluster (n int) engine=Log"
    )
