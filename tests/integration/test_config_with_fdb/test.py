import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from textwrap import dedent
from xml.dom import minidom
import time
import os


cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        node = cluster.add_instance(
            'node',
            base_config_dir='configs',
            main_configs=['configs/config.d/foundationdb.xml', 'configs/config.d/change.xml'],
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)
        yield cluster
    finally:
        cluster.shutdown()


def test_instance_config_should_be_persisted_on_fdb(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'logger.size'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1000M\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1000M\n"

    node.stop_clickhouse()
    node.exec_in_container(["bash", "-c", "rm /etc/clickhouse-server/config.d/change.xml"])
    node.start_clickhouse()

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1000M\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1000M\n"


def test_set_existent_instance_config_value_with_fdb(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'mark_cache_size'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "5368709120\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "5368709120\n"

    node.query(f"set config {instance_config_name} = 5370000000")
    time.sleep(1)

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "5370000000\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "5370000000\n"


def test_set_nonexistent_instance_config_value_with_fdb(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'disable_internal_dns_cache'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == ""
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == ""

    node.query(f"set config {instance_config_name} = 1")
    time.sleep(1)

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1\n"


def test_set_existent_instance_config_none_with_fdb(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'disable_internal_dns_cache'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1\n"

    node.query(f"set config {instance_config_name} NONE")
    time.sleep(1)

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == ""
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == ""


def test_set_nonexistent_instance_config_none_with_fdb(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'disable_internal_dns_cache'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == ""
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == ""

    with pytest.raises(Exception, match=r"DB::Exception: Key '.*' doesn't exist in FoundationDB"):
        node.query(f"set config {instance_config_name} NONE")


def test_stop_fdb_while_set_instance_config(start_cluster):
    node = start_cluster.instances["node"]
    instance_config_name = 'logger.size'
    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1000M\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1000M\n"

    cluster.stop_fdb()

    with pytest.raises(Exception, match="Operation aborted because the transaction timed out"):
        node.query(f"set config {instance_config_name} = '1100M'")

    cluster.start_fdb()

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1000M\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1000M\n"

    node.query(f"set config {instance_config_name} = '1100M'")

    assert node.query(f"select value from system.configs where name = '{instance_config_name}'") == "1100M\n"
    assert node.query(f"select value from system.foundationdb where type = 'config' and key = ['{instance_config_name}']") == "1100M\n"


def test_stop_fdb(start_cluster):
    node = start_cluster.instances["node"]
    node.stop_clickhouse()
    cluster.stop_fdb()
    with pytest.raises(Exception, match="Cannot start ClickHouse"):
        node.start_clickhouse()
    assert node.contains_in_log("Operation aborted because the transaction timed out")





