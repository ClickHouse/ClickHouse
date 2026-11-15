import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)
node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/config.d/clusters.xml"],
    user_configs=["configs/users.d/default_with_password.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node, shard in [
            (node1, 1),
            (node2, 1),
            (node3, 2),
            (node4, 2),
            (node5, 3),
            (node6, 3),
        ]:
            node.query(
                """
                    CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                    ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}')
                    PARTITION BY date
                    ORDER BY id
                """.format(
                    shard=shard, replica=node.name
                ),
                settings={"password": "clickhouse"},
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_truncate(start_cluster):
    node1.query(
        "insert into test_table values ('2019-02-15', 1, 2), ('2019-02-15', 2, 3), ('2019-02-15', 3, 4)",
        settings={"password": "clickhouse"},
    )

    assert (
        node1.query(
            "select count(*) from test_table", settings={"password": "clickhouse"}
        )
        == "3\n"
    )
    node2.query("system sync replica test_table", settings={"password": "clickhouse"})
    assert (
        node2.query(
            "select count(*) from test_table", settings={"password": "clickhouse"}
        )
        == "3\n"
    )

    node3.query(
        "insert into test_table values ('2019-02-16', 1, 2), ('2019-02-16', 2, 3), ('2019-02-16', 3, 4)",
        settings={"password": "clickhouse"},
    )

    assert (
        node3.query(
            "select count(*) from test_table", settings={"password": "clickhouse"}
        )
        == "3\n"
    )
    node4.query("system sync replica test_table", settings={"password": "clickhouse"})
    assert (
        node4.query(
            "select count(*) from test_table", settings={"password": "clickhouse"}
        )
        == "3\n"
    )

    node3.query(
        "truncate table test_table on cluster 'awesome_cluster'",
        settings={"password": "clickhouse"},
    )

    for node in [node1, node2, node3, node4]:
        assert_eq_with_retry(
            node,
            "select count(*) from test_table",
            "0",
            settings={"password": "clickhouse"},
        )

    node2.query(
        "drop table test_table on cluster 'awesome_cluster'",
        settings={"password": "clickhouse"},
    )

    for node in [node1, node2, node3, node4]:
        assert_eq_with_retry(
            node,
            "select count(*) from system.tables where name='test_table'",
            "0",
            settings={"password": "clickhouse"},
        )


def test_alter(start_cluster):
    node5.query(
        "insert into test_table values ('2019-02-15', 1, 2), ('2019-02-15', 2, 3), ('2019-02-15', 3, 4)",
        settings={"password": "clickhouse"},
    )
    node6.query(
        "insert into test_table values ('2019-02-15', 4, 2), ('2019-02-15', 5, 3), ('2019-02-15', 6, 4)",
        settings={"password": "clickhouse"},
    )

    node5.query("SYSTEM SYNC REPLICA test_table", settings={"password": "clickhouse"})
    node6.query("SYSTEM SYNC REPLICA test_table", settings={"password": "clickhouse"})

    assert_eq_with_retry(
        node5,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )

    node6.query(
        "OPTIMIZE TABLE test_table ON CLUSTER 'simple_cluster' FINAL",
        settings={"password": "clickhouse"},
    )

    node5.query("SYSTEM SYNC REPLICA test_table", settings={"password": "clickhouse"})
    node6.query("SYSTEM SYNC REPLICA test_table", settings={"password": "clickhouse"})

    assert_eq_with_retry(
        node5,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )

    node6.query(
        "ALTER TABLE test_table ON CLUSTER 'simple_cluster' DETACH PARTITION '2019-02-15'",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node5,
        "select count(*) from test_table",
        "0",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select count(*) from test_table",
        "0",
        settings={"password": "clickhouse"},
    )

    with pytest.raises(QueryRuntimeException):
        node6.query(
            "ALTER TABLE test_table ON CLUSTER 'simple_cluster' ATTACH PARTITION '2019-02-15'",
            settings={"password": "clickhouse"},
        )

    node5.query(
        "ALTER TABLE test_table ATTACH PARTITION '2019-02-15'",
        settings={"password": "clickhouse"},
    )

    assert_eq_with_retry(
        node5,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select count(*) from test_table",
        "6",
        settings={"password": "clickhouse"},
    )

    node5.query(
        "ALTER TABLE test_table ON CLUSTER 'simple_cluster' MODIFY COLUMN dummy String",
        settings={"password": "clickhouse"},
    )

    assert_eq_with_retry(
        node5,
        "select length(dummy) from test_table ORDER BY dummy LIMIT 1",
        "1",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select length(dummy) from test_table ORDER BY dummy LIMIT 1",
        "1",
        settings={"password": "clickhouse"},
    )

    node6.query(
        "ALTER TABLE test_table ON CLUSTER 'simple_cluster' DROP PARTITION '2019-02-15'",
        settings={"password": "clickhouse"},
    )

    assert_eq_with_retry(
        node5,
        "select count(*) from test_table",
        "0",
        settings={"password": "clickhouse"},
    )
    assert_eq_with_retry(
        node6,
        "select count(*) from test_table",
        "0",
        settings={"password": "clickhouse"},
    )
