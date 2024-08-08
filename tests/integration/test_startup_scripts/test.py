import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts1.xml",
    ],
    with_zookeeper=False,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts2.xml",
    ],
    with_zookeeper=False,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts3.xml",
    ],
    with_zookeeper=False,
)

node4 = cluster.add_instance(
    "node4",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts4.xml",
    ],
    with_zookeeper=False,
)
node5 = cluster.add_instance(
    "node5",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts5.xml",
    ],
    with_zookeeper=False,
)
node6 = cluster.add_instance(
    "node6",
    main_configs=[
        "configs/config.d/query_log.xml",
        "configs/config.d/startup_scripts6_1.xml",
        "configs/config.d/startup_scripts6_2.xml",
    ],
    with_zookeeper=False,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    yield cluster
    cluster.shutdown()


def test_startup_scripts_1():
    # no exceptions
    assert node.query("SHOW TABLES") == "TestTable_1\nTestTable_2\nTestTable_4\n"


def test_startup_scripts_2():
    # exception in condition
    assert node2.query("SHOW TABLES") == "TestTable_1\nTestTable_3\n"


def test_startup_scripts_3():
    # exception in query
    assert node3.query("SHOW TABLES") == "TestTable_1\n"


def test_startup_scripts_4():
    # exception in condition and query
    assert node4.query("SHOW TABLES") == "TestTable_1\n"


def test_startup_scripts_5():
    # empty startup_scripts
    assert node5.query("SHOW TABLES") == ""


def test_startup_scripts_6():
    # two <startup_scripts> sections and two files
    # result:
    # 'TestTable_5\nTestTable_6\n'
    assert (
        node6.query("SHOW TABLES")
        == "TestTable_1\nTestTable_2\nTestTable_3\nTestTable_4\nTestTable_5\nTestTable_6\n"
    )
