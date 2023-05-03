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
        cluster = ClickHouseCluster(__file__, name="fdb_down")
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
    with open(os.path.dirname(__file__) + "/config/foundationdb.xml", "r") as f:
            node.replace_config("/etc/clickhouse-server/config.d/foundationdb.xml", f.read())
    copy_file_to_container(os.path.join(SCRIPT_DIR, 'dictionaries/.'), '/etc/clickhouse-server/dictionaries', node.docker_id)
    node.query("CREATE TABLE dictionary_values (id UInt64, value_1 String, value_2 String) ENGINE=TinyLog;")
    node.query("INSERT INTO dictionary_values VALUES (0, 'Value_1', 'Value_2')")
    change_config("*_dictionary.xml", node)
    node.restart_clickhouse()
    assert node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));") == 'Value_1\n'
    assert node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));") == 'Value_2\n'
    nums = node.query(f"SELECT count(*) from system.dictionaries")
    assert nums.strip() == '3'

    started_cluster.stop_fdb()
    # time.sleep(60)
    # # assert node.contains_in_log("Operation aborted because the transaction timed out")
    # nums = node.query(f"SELECT count(*) from system.dictionaries")
    # assert nums.strip() == '0'
    started_cluster.start_fdb()
    time.sleep(60)
    assert node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));") == 'Value_1\n'
    assert node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));") == 'Value_2\n'
    nums = node.query(f"SELECT count(*) from system.dictionaries")
    assert nums.strip() == '3'

