import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
import os


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/storage.xml"],
    tmpfs=["/disk:size=100M"],
    macros={"replica": "node1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/storage.xml"],
    tmpfs=["/disk:size=100M"],
    macros={"replica": "node2"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def copy_keys(instance, keys_file_name):
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, f"configs/{keys_file_name}.xml"),
        "/etc/clickhouse-server/config.d/z_keys.xml",
    )
    instance.query("SYSTEM RELOAD CONFIG")


def create_table():
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' NO DELAY")
    node1.query(
        """
        CREATE TABLE tbl ON CLUSTER 'cluster' (
            id Int64,
            str String
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')
        ORDER BY id
        SETTINGS storage_policy='encrypted_policy'
        """
    )


def insert_data():
    node1.query("INSERT INTO tbl VALUES (1, 'str1')")
    node2.query("INSERT INTO tbl VALUES (1, 'str1')")  # Test deduplication
    node2.query("INSERT INTO tbl VALUES (2, 'str2')")


def optimize_table():
    node1.query("OPTIMIZE TABLE tbl ON CLUSTER 'cluster' FINAL")


def check_table():
    expected = [[1, "str1"], [2, "str2"]]
    assert node1.query("SELECT * FROM tbl ORDER BY id") == TSV(expected)
    assert node2.query("SELECT * FROM tbl ORDER BY id") == TSV(expected)
    assert node1.query("CHECK TABLE tbl") == "1\n"
    assert node2.query("CHECK TABLE tbl") == "1\n"


# Actual tests:


def test_same_keys():
    copy_keys(node1, "key_a")
    copy_keys(node2, "key_a")
    create_table()

    insert_data()
    check_table()

    optimize_table()
    check_table()


def test_different_keys():
    copy_keys(node1, "key_a")
    copy_keys(node2, "key_b")
    create_table()

    insert_data()
    check_table()

    optimize_table()
    check_table()
