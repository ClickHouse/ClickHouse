import time

import pytest

from helpers.cluster import ClickHouseCluster


def _fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                CREATE DATABASE test;
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/credentials1.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/credentials1.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def same_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node1, node2], 1)

        yield cluster

    finally:
        cluster.shutdown()


def test_same_credentials(same_credentials_cluster):
    node1.query("insert into test_table values ('2017-06-16', 111, 0)")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == "111\n"
    assert node2.query("SELECT id FROM test_table order by id") == "111\n"

    node2.query("insert into test_table values ('2017-06-17', 222, 1)")
    time.sleep(1)

    assert node1.query("SELECT id FROM test_table order by id") == "111\n222\n"
    assert node2.query("SELECT id FROM test_table order by id") == "111\n222\n"


node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml", "configs/no_credentials.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml", "configs/no_credentials.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def no_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node3, node4], 2)

        yield cluster

    finally:
        cluster.shutdown()


def test_no_credentials(no_credentials_cluster):
    node3.query("insert into test_table values ('2017-06-18', 111, 0)")
    time.sleep(1)

    assert node3.query("SELECT id FROM test_table order by id") == "111\n"
    assert node4.query("SELECT id FROM test_table order by id") == "111\n"

    node4.query("insert into test_table values ('2017-06-19', 222, 1)")
    time.sleep(1)

    assert node3.query("SELECT id FROM test_table order by id") == "111\n222\n"
    assert node4.query("SELECT id FROM test_table order by id") == "111\n222\n"


node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/remote_servers.xml", "configs/credentials1.xml"],
    with_zookeeper=True,
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/remote_servers.xml", "configs/credentials2.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def different_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node5, node6], 3)

        yield cluster

    finally:
        cluster.shutdown()


def test_different_credentials(different_credentials_cluster):
    node5.query("insert into test_table values ('2017-06-20', 111, 0)")
    time.sleep(1)

    assert node5.query("SELECT id FROM test_table order by id") == "111\n"
    assert node6.query("SELECT id FROM test_table order by id") == ""

    node6.query("insert into test_table values ('2017-06-21', 222, 1)")
    time.sleep(1)

    assert node5.query("SELECT id FROM test_table order by id") == "111\n"
    assert node6.query("SELECT id FROM test_table order by id") == "222\n"

    add_old = """
    <clickhouse>
        <interserver_http_port>9009</interserver_http_port>
        <interserver_http_credentials>
            <user>admin</user>
            <password>222</password>
            <old>
                <user>root</user>
                <password>111</password>
            </old>
            <old>
                <user>aaa</user>
                <password>333</password>
            </old>
        </interserver_http_credentials>
    </clickhouse>
    """

    node5.replace_config("/etc/clickhouse-server/config.d/credentials1.xml", add_old)

    node5.query("SYSTEM RELOAD CONFIG")
    node5.query("INSERT INTO test_table values('2017-06-21', 333, 1)")
    node6.query("SYSTEM SYNC REPLICA test_table", timeout=10)

    assert node6.query("SELECT id FROM test_table order by id") == "111\n222\n333\n"


node7 = cluster.add_instance(
    "node7",
    main_configs=["configs/remote_servers.xml", "configs/credentials1.xml"],
    with_zookeeper=True,
)
node8 = cluster.add_instance(
    "node8",
    main_configs=["configs/remote_servers.xml", "configs/no_credentials.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def credentials_and_no_credentials_cluster():
    try:
        cluster.start()

        _fill_nodes([node7, node8], 4)

        yield cluster

    finally:
        cluster.shutdown()


def test_credentials_and_no_credentials(credentials_and_no_credentials_cluster):
    node7.query("insert into test_table values ('2017-06-21', 111, 0)")
    time.sleep(1)

    assert node7.query("SELECT id FROM test_table order by id") == "111\n"
    assert node8.query("SELECT id FROM test_table order by id") == ""

    node8.query("insert into test_table values ('2017-06-22', 222, 1)")
    time.sleep(1)

    assert node7.query("SELECT id FROM test_table order by id") == "111\n"
    assert node8.query("SELECT id FROM test_table order by id") == "222\n"

    allow_empty = """
    <clickhouse>
        <interserver_http_port>9009</interserver_http_port>
        <interserver_http_credentials>
            <user>admin</user>
            <password>222</password>
            <allow_empty>true</allow_empty>
        </interserver_http_credentials>
    </clickhouse>
    """

    # change state: Flip node7 to mixed auth/non-auth (allow node8)
    node7.replace_config(
        "/etc/clickhouse-server/config.d/credentials1.xml", allow_empty
    )

    node7.query("SYSTEM RELOAD CONFIG")
    node7.query("insert into test_table values ('2017-06-22', 333, 1)")
    node8.query("SYSTEM SYNC REPLICA test_table", timeout=10)
    assert node8.query("SELECT id FROM test_table order by id") == "111\n222\n333\n"
