import logging
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", stay_alive=True, main_configs=["config/dictionaries_config.xml"]
)


def copy_file_to_container(local_path, dist_path, container_id):
    os.system(
        "docker cp {local} {cont_id}:{dist}".format(
            local=local_path, cont_id=container_id, dist=dist_path
        )
    )


config = """<clickhouse>
    <dictionaries_config>/etc/clickhouse-server/dictionaries/{dictionaries_config}</dictionaries_config>
</clickhouse>"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        copy_file_to_container(
            os.path.join(SCRIPT_DIR, "dictionaries/."),
            "/etc/clickhouse-server/dictionaries",
            node.docker_id,
        )

        node.query(
            "CREATE TABLE dictionary_values (id UInt64, value_1 String, value_2 String) ENGINE=TinyLog;"
        )
        node.query("INSERT INTO dictionary_values VALUES (0, 'Value_1', 'Value_2')")

        node.restart_clickhouse()

        yield cluster

    finally:
        cluster.shutdown()


def change_config(dictionaries_config):
    node.replace_config(
        "/etc/clickhouse-server/config.d/dictionaries_config.xml",
        config.format(dictionaries_config=dictionaries_config),
    )
    node.query("SYSTEM RELOAD CONFIG;")


def test(started_cluster):
    # Set config with the path to the first dictionary.
    change_config("dictionary_config.xml")

    time.sleep(10)

    assert (
        node.query("SELECT dictGet('test_dictionary_1', 'value_1', toUInt64(0));")
        == "Value_1\n"
    )

    # Change path to the second dictionary in config.
    change_config("dictionary_config2.xml")

    time.sleep(10)

    # Check that the new dictionary is loaded.
    assert (
        node.query("SELECT dictGet('test_dictionary_2', 'value_2', toUInt64(0));")
        == "Value_2\n"
    )

    # Check that the previous dictionary was unloaded.
    node.query_and_get_error(
        "SELECT dictGet('test_dictionary_1', 'value', toUInt64(0));"
    )
