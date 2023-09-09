import os
from pdb import Restart
import sys
import time
import logging
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

def copy_file_to_container(local_path, dist_path, container_id):
    os.system("docker cp {local} {cont_id}:{dist}".format(local=local_path, cont_id=container_id, dist=dist_path))

def change_config(dictionaries_config, node):
    node.replace_config("/etc/clickhouse-server/config.d/dictionaries_config.xml", config.format(dictionaries_config=dictionaries_config))
    node.query("SYSTEM RELOAD CONFIG;")

config = '''<clickhouse>
    <dictionaries_config>/etc/clickhouse-server/dictionaries/{dictionaries_config}</dictionaries_config>
</clickhouse>'''

@pytest.fixture(scope="module")
def started_cluster(request):
    try:
        cluster = ClickHouseCluster(__file__)
        node = cluster.add_instance(
            'node',
            with_foundationdb=True,
            stay_alive=True
        )
        cluster.start(destroy_dirs=True)
        yield cluster

    finally:
        cluster.shutdown()

def test(started_cluster):
    node = started_cluster.instances["node"]
    copy_file_to_container(os.path.join(SCRIPT_DIR, 'dictionaries/.'), '/etc/clickhouse-server/dictionaries', node.docker_id)
    node.query("CREATE TABLE dictionary_values (id UInt64, value_1 String, value_2 String, value_3 String) ENGINE=TinyLog;")
    node.query("INSERT INTO dictionary_values VALUES (0, 'Value_1', 'Value_2', 'Value_3')")
    change_config("*_dictionary.xml", node)
    node.restart_clickhouse()
    dict_name = "/etc/clickhouse-server/dictionaries/dictionary_config.xml"
    # Boot without fdb.
    assert node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));") == 'Value_1\n'
    assert node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));") == 'Value_2\n'
    assert node.query("SELECT dictGet('test_dictionary_3', 'value_3', toUInt64(0));") == 'Value_3\n'

    nums = node.query(f"SELECT count(*) from system.dictionaries")
    assert nums.strip() == '3'
    node.stop_clickhouse()
    # First boot with fdb.
    with open(os.path.dirname(__file__) + "/config/foundationdb.xml", "r") as f:
        node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    node.start_clickhouse()
    assert node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));") == 'Value_1\n'
    assert node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));") == 'Value_2\n'
    assert node.query("SELECT dictGet('test_dictionary_3', 'value_3', toUInt64(0));") == 'Value_3\n'

    nums = node.query(f"SELECT count(*) from system.dictionaries")
    assert nums.strip() == '3'
    # # Delete local file, second boot with fdb.
    node.exec_in_container(["bash", "-c", f"rm -rf dictionaries"])
    node.start_clickhouse()
    assert node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));") == 'Value_1\n'
    assert node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));") == 'Value_2\n'
    assert node.query("SELECT dictGet('test_dictionary_3', 'value_3', toUInt64(0));") == 'Value_3\n'

    nums = node.query(f"SELECT count(*) from system.dictionaries")
    assert nums.strip() == '3'


