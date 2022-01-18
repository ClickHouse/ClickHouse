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


config = '''<yandex>
    <models_config>/etc/clickhouse-server/model/{model_config}</models_config>
</yandex>'''


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        copy_file_to_container(os.path.join(SCRIPT_DIR, 'model/.'), '/etc/clickhouse-server/model', node.docker_id)
        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def change_config(model_config):
    node.replace_config("/etc/clickhouse-server/config.d/models_config.xml", config.format(model_config=model_config))
    node.query("SYSTEM RELOAD CONFIG;")


def test(started_cluster):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")

    # Set config with the path to the first model.
    change_config("model_config.xml")

    node.query("SELECT modelEvaluate('model1', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);")

    # Change path to the second model in config.
    change_config("model_config2.xml")

    # Check that the new model is loaded.
    node.query("SELECT modelEvaluate('model2', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);")

    # Check that the old model was unloaded.
    node.query_and_get_error("SELECT modelEvaluate('model1', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);")

