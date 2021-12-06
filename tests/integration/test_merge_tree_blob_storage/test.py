import logging
import random
import string
import time
import threading
import os

import pytest
from helpers.cluster import ClickHouseCluster, get_instances_dir


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, './{}/node/configs/config.d/storage_conf.xml'.format(get_instances_dir()))

NODE_NAME = "node"
TABLE_NAME = "blob_storage_test"


# TODO: move these functions copied from s3 to an utility file?
def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(NODE_NAME,
            main_configs=["configs/config.d/storage_conf.xml", "configs/config.d/bg_processing_pool_conf.xml"],
            with_azurite=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "old_parts_lifetime": 1, # TODO: setting to 0 causes errors
        "index_granularity": 512
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {table_name} (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}"""

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(create_table_statement)
    assert node.query(f"SELECT COUNT(*) FROM {table_name} FORMAT Values") == "(0)"


def test_create_table(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)


def test_simple_insert_select(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)
    node.query(f"INSERT INTO {TABLE_NAME} VALUES ('2021-11-13', 3, 'hello')")
    assert node.query(f"SELECT dt, id, data FROM {TABLE_NAME} FORMAT Values") == "('2021-11-13',3,'hello')"


def test_inserts_selects(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    values1 = generate_values('2020-01-03', 4096)
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {values1}")
    assert node.query(f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values") == values1

    values2 = generate_values('2020-01-04', 4096)
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {values2}")
    assert node.query(f"SELECT * FROM {TABLE_NAME} ORDER BY dt, id FORMAT Values") == values1 + "," + values2

    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} where id = 1 FORMAT Values") == "(2)"


@pytest.mark.parametrize(
    "merge_vertical", [
        (True),
        (False),
])
def test_insert_same_partition_and_merge(cluster, merge_vertical):
    settings = {}
    if merge_vertical:
        settings['vertical_merge_algorithm_min_rows_to_activate'] = 0
        settings['vertical_merge_algorithm_min_columns_to_activate'] = 0

    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME, **settings)

    node.query(f"SYSTEM STOP MERGES {TABLE_NAME}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 1024)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 2048)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 1024, -1)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 2048, -1)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096, -1)}")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(distinct(id)) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"SYSTEM START MERGES {TABLE_NAME}")

    # TODO: crashes at "Failed to read metadata file. (UNKNOWN_FORMAT)"
    # Wait for merges and old parts deletion
    for attempt in range(0, 10):
        parts_count = node.query(f"SELECT COUNT(*) FROM system.parts WHERE table = '{TABLE_NAME}' FORMAT Values")
        print("parts_count: ", parts_count)
        if parts_count == "(1)":
            break

        if attempt == 9:
            assert parts_count == "(1)"

        time.sleep(10)

    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(distinct(id)) FROM {TABLE_NAME} FORMAT Values") == "(8192)"
