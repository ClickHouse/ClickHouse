import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True, main_configs=['config/models_config.xml'])


def copy_file_to_container(local_path, dist_path, container_id):
    os.system("docker cp {local} {cont_id}:{dist}".format(local=local_path, cont_id=container_id, dist=dist_path))


@pytest.fixture(scope="module")
def started_cluster():
    os.system("cat {} > {}".format(os.path.join(SCRIPT_DIR, 'model/lib_part_*'), os.path.join(SCRIPT_DIR, 'model/libtensorflowlite_c.so')))
    try:
        cluster.start()

        copy_file_to_container(os.path.join(SCRIPT_DIR, 'model/.'), '/etc/clickhouse-server/model', node.docker_id)
        node.restart_clickhouse()

        yield cluster

    finally:
        os.system("rm {}".format(os.path.join(SCRIPT_DIR, 'model/libtensorflowlite_c.so')))
        cluster.shutdown()


def test(started_cluster):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    result = node.query("select modelEvaluate('test', 42);")
    assert result == "43\n"

    result = node.query("SELECT sum(abs(x + 1 - modelEvaluate('test', \"x\"))) as result from (SELECT round(rand32() / 2) AS x FROM numbers(100000));")
    assert result == "0\n"

