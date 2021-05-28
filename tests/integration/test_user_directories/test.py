import pytest
import os
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        node.exec_in_container("cp /etc/clickhouse-server/users.xml /etc/clickhouse-server/users2.xml")
        node.exec_in_container("cp /etc/clickhouse-server/users.xml /etc/clickhouse-server/users3.xml")
        node.exec_in_container("cp /etc/clickhouse-server/users.xml /etc/clickhouse-server/users4.xml")
        node.exec_in_container("cp /etc/clickhouse-server/users.xml /etc/clickhouse-server/users5.xml")

        yield cluster

    finally:
        cluster.shutdown()


def test_old_style():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/old_style.xml"), '/etc/clickhouse-server/config.d/z.xml')
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV([["users.xml",       "users.xml",       "/etc/clickhouse-server/users2.xml", 1, 1],
                                                                       ["local directory", "local directory", "/var/lib/clickhouse/access2/",      0, 2]])


def test_local_directories():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/local_directories.xml"), '/etc/clickhouse-server/config.d/z.xml')
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV([["users.xml",            "users.xml",       "/etc/clickhouse-server/users3.xml", 1, 1],
                                                                       ["local directory",      "local directory", "/var/lib/clickhouse/access3/",      0, 2],
                                                                       ["local directory (ro)", "local directory", "/var/lib/clickhouse/access3-ro/",   1, 3]])


def test_relative_path():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/relative_path.xml"), '/etc/clickhouse-server/config.d/z.xml')
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV([["users.xml", "users.xml", "/etc/clickhouse-server/users4.xml", 1, 1]])


def test_memory():
    node.copy_file_to_container(os.path.join(SCRIPT_DIR, "configs/memory.xml"), '/etc/clickhouse-server/config.d/z.xml')
    node.restart_clickhouse()
    assert node.query("SELECT * FROM system.user_directories") == TSV([["users.xml", "users.xml", "/etc/clickhouse-server/users5.xml", 1, 1],
                                                                       ["memory",    "memory",    "",                                  0, 2]])
