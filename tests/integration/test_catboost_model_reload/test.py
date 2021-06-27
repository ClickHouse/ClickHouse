import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', stay_alive=True, main_configs=['config/models_config.xml', 'config/catboost_lib.xml'])

def copy_file_to_container(local_path, dist_path, container_id):
    os.system("docker cp {local} {cont_id}:{dist}".format(local=local_path, cont_id=container_id, dist=dist_path))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        copy_file_to_container(os.path.join(SCRIPT_DIR, 'model/.'), '/etc/clickhouse-server/model', node.docker_id)
        node.query("CREATE TABLE binary (x UInt64, y UInt64) ENGINE = TinyLog()")
        node.query("INSERT INTO binary VALUES (1, 1), (1, 0), (0, 1), (0, 0)")

        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()

def test_model_reload(started_cluster):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    node.exec_in_container(["bash", "-c", "rm -f /etc/clickhouse-server/model/model.cbm"])
    node.exec_in_container(["bash", "-c", "ln /etc/clickhouse-server/model/conjunction.cbm /etc/clickhouse-server/model/model.cbm"])
    node.query("SYSTEM RELOAD MODEL model")

    result = node.query("""
        WITH modelEvaluate('model', toFloat64(x), toFloat64(y)) as prediction, exp(prediction) / (1 + exp(prediction)) as probability
        SELECT if(probability > 0.5, 1, 0) FROM binary;
        """)
    assert result == '1\n0\n0\n0\n'

    node.exec_in_container(["bash", "-c", "rm /etc/clickhouse-server/model/model.cbm"])
    node.exec_in_container(["bash", "-c", "ln /etc/clickhouse-server/model/disjunction.cbm /etc/clickhouse-server/model/model.cbm"])
    node.query("SYSTEM RELOAD MODEL model")

    result = node.query("""
        WITH modelEvaluate('model', toFloat64(x), toFloat64(y)) as prediction, exp(prediction) / (1 + exp(prediction)) as probability
        SELECT if(probability > 0.5, 1, 0) FROM binary;
        """)
    assert result == '1\n1\n1\n0\n'

def test_models_reload(started_cluster):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    node.exec_in_container(["bash", "-c", "rm -f /etc/clickhouse-server/model/model.cbm"])
    node.exec_in_container(["bash", "-c", "ln /etc/clickhouse-server/model/conjunction.cbm /etc/clickhouse-server/model/model.cbm"])
    node.query("SYSTEM RELOAD MODELS")

    result = node.query("""
        WITH modelEvaluate('model', toFloat64(x), toFloat64(y)) as prediction, exp(prediction) / (1 + exp(prediction)) as probability
        SELECT if(probability > 0.5, 1, 0) FROM binary;
        """)
    assert result == '1\n0\n0\n0\n'

    node.exec_in_container(["bash", "-c", "rm /etc/clickhouse-server/model/model.cbm"])
    node.exec_in_container(["bash", "-c", "ln /etc/clickhouse-server/model/disjunction.cbm /etc/clickhouse-server/model/model.cbm"])
    node.query("SYSTEM RELOAD MODELS")

    result = node.query("""
        WITH modelEvaluate('model', toFloat64(x), toFloat64(y)) as prediction, exp(prediction) / (1 + exp(prediction)) as probability
        SELECT if(probability > 0.5, 1, 0) FROM binary;
        """)
    assert result == '1\n1\n1\n0\n'
