import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/user_good_restricted.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/user_good_restricted.xml"],
    with_zookeeper=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/user_good_allowed.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/user_good_allowed.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query(
                """
            CREATE TABLE sometable(date Date, id UInt32, value Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/sometable', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
                """.format(
                    replica=node.name
                ),
                user="awesome",
            )

        for node in [node3, node4]:
            node.query(
                """
            CREATE TABLE someothertable(date Date, id UInt32, value Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/someothertable', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
                """.format(
                    replica=node.name
                ),
                user="good",
            )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "table,query,expected,n1,n2",
    [
        pytest.param(
            "sometable",
            "ALTER TABLE sometable DROP PARTITION 201706",
            "1",
            node1,
            node2,
            id="case1",
        ),
        pytest.param(
            "sometable", "TRUNCATE TABLE sometable", "0", node1, node2, id="case2"
        ),
        pytest.param(
            "sometable", "OPTIMIZE TABLE sometable", "4", node1, node2, id="case3"
        ),
        pytest.param(
            "someothertable",
            "ALTER TABLE someothertable DROP PARTITION 201706",
            "1",
            node3,
            node4,
            id="case4",
        ),
        pytest.param(
            "someothertable",
            "TRUNCATE TABLE someothertable",
            "0",
            node3,
            node4,
            id="case5",
        ),
        pytest.param(
            "someothertable",
            "OPTIMIZE TABLE someothertable",
            "4",
            node3,
            node4,
            id="case6",
        ),
    ],
)
def test_alter_table_drop_partition(started_cluster, table, query, expected, n1, n2):
    to_insert = """\
2017-06-16	111	0
2017-06-16	222	1
2017-06-16	333	2
2017-07-16	444	3
"""
    n1.query("INSERT INTO {} FORMAT TSV".format(table), stdin=to_insert, user="good")

    assert_eq_with_retry(n1, "SELECT COUNT(*) from {}".format(table), "4", user="good")
    assert_eq_with_retry(n2, "SELECT COUNT(*) from {}".format(table), "4", user="good")

    ### It maybe leader and everything will be ok
    n1.query(query, user="good")

    assert_eq_with_retry(
        n1, "SELECT COUNT(*) from {}".format(table), expected, user="good"
    )
    assert_eq_with_retry(
        n2, "SELECT COUNT(*) from {}".format(table), expected, user="good"
    )

    n1.query("INSERT INTO {} FORMAT TSV".format(table), stdin=to_insert, user="good")

    assert_eq_with_retry(n1, "SELECT COUNT(*) from {}".format(table), "4", user="good")
    assert_eq_with_retry(n2, "SELECT COUNT(*) from {}".format(table), "4", user="good")

    ### If node1 is leader than node2 will be slave
    n2.query(query, user="good")

    assert_eq_with_retry(
        n1, "SELECT COUNT(*) from {}".format(table), expected, user="good"
    )
    assert_eq_with_retry(
        n2, "SELECT COUNT(*) from {}".format(table), expected, user="good"
    )
