import logging
import random
import string
import time
import threading
import os

import pytest
from helpers.cluster import ClickHouseCluster, get_instances_dir


# By default the exceptions that was throwed in threads will be ignored
# (they will not mark the test as failed, only printed to stderr).
#
# Wrap thrading.Thread and re-throw exception on join()
class SafeThread(threading.Thread):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.exception = None
    def run(self):
        try:
            self.target()
        except Exception as e: # pylint: disable=broad-except
            self.exception = e
    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, './{}/node/configs/config.d/storage_conf.xml'.format(get_instances_dir()))

NODE_NAME = "node"
TABLE_NAME = "blob_storage_table"
BLOB_STORAGE_DISK = "blob_storage_disk"
LOCAL_DISK = "hdd"


# TODO: these tests resemble S3 tests a lot, utility functions and tests themselves could be abstracted

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def replace_config(old, new):
    config = open(CONFIG_PATH, 'r')
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(CONFIG_PATH, 'w')
    config.writelines(config_lines)
    config.close()


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
        "old_parts_lifetime": 1,
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

    # Wait for merges and old parts deletion
    for attempt in range(0, 10):
        parts_count = node.query(f"SELECT COUNT(*) FROM system.parts WHERE table = '{TABLE_NAME}' FORMAT Values")
        if parts_count == "(1)":
            break

        if attempt == 9:
            assert parts_count == "(1)"

        time.sleep(1)

    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(distinct(id)) FROM {TABLE_NAME} FORMAT Values") == "(8192)"


def test_alter_table_columns(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096, -1)}")

    node.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have been merged
    node.query(f"OPTIMIZE TABLE {TABLE_NAME}")

    assert node.query(f"SELECT sum(col1) FROM {TABLE_NAME} FORMAT Values") == "(8192)"
    assert node.query(f"SELECT sum(col1) FROM {TABLE_NAME} WHERE id > 0 FORMAT Values") == "(4096)"

    node.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN col1 String", settings={"mutations_sync": 2})

    assert node.query(f"SELECT distinct(col1) FROM {TABLE_NAME} FORMAT Values") == "('1')"


def test_attach_detach_partition(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-03'")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(4096)"

    node.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION '2020-01-03'")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"ALTER TABLE {TABLE_NAME} DROP PARTITION '2020-01-03'")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(4096)"

    node.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-04'")
    node.query(f"ALTER TABLE {TABLE_NAME} DROP DETACHED PARTITION '2020-01-04'", settings={"allow_drop_detached": 1})
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(0)"


def test_move_partition_to_another_disk(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{LOCAL_DISK}'")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{BLOB_STORAGE_DISK}'")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"


def test_table_manipulations(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    renamed_table = TABLE_NAME + "_renamed"

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}")

    node.query(f"RENAME TABLE {TABLE_NAME} TO {renamed_table}")
    assert node.query(f"SELECT count(*) FROM {renamed_table} FORMAT Values") == "(8192)"

    node.query(f"RENAME TABLE {renamed_table} TO {TABLE_NAME}")
    assert node.query(f"CHECK TABLE {TABLE_NAME} FORMAT Values") == "(1)"

    node.query(f"DETACH TABLE {TABLE_NAME}")
    node.query(f"ATTACH TABLE {TABLE_NAME}")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"

    node.query(f"TRUNCATE TABLE {TABLE_NAME}")
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(0)"


def test_move_replace_partition_to_another_table(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    table_clone_name = TABLE_NAME + "_clone"

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 4096, -1)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-06', 4096, -1)}")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(16384)"

    create_table(node, table_clone_name)

    node.query(f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-03' TO TABLE {table_clone_name}")
    node.query(f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-05' TO TABLE {table_clone_name}")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(8192)"
    assert node.query(f"SELECT sum(id) FROM {table_clone_name} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {table_clone_name} FORMAT Values") == "(8192)"

    # Add new partitions to source table, but with different values and replace them from copied table.
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096, -1)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 4096)}")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(16384)"

    node.query(f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-03' FROM {table_clone_name}")
    node.query(f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-05' FROM {table_clone_name}")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(16384)"
    assert node.query(f"SELECT sum(id) FROM {table_clone_name} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {table_clone_name} FORMAT Values") == "(8192)"

    node.query(f"DROP TABLE {table_clone_name} NO DELAY")
    assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert node.query(f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(16384)"

    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE")

    node.query(f"DROP TABLE {TABLE_NAME} NO DELAY")


def test_freeze_unfreeze(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    backup1 = 'backup1'
    backup2 = 'backup2'

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")
    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup1}'")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}")
    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup2}'")

    node.query(f"TRUNCATE TABLE {TABLE_NAME}")

    # Unfreeze single partition from backup1.
    node.query(f"ALTER TABLE {TABLE_NAME} UNFREEZE PARTITION '2020-01-03' WITH NAME '{backup1}'")
    # Unfreeze all partitions from backup2.
    node.query(f"ALTER TABLE {TABLE_NAME} UNFREEZE WITH NAME '{backup2}'")


def test_apply_new_settings(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}")

    # Force multi-part upload mode.
    replace_config("<max_single_part_upload_size>33554432</max_single_part_upload_size>",
                   "<max_single_part_upload_size>4096</max_single_part_upload_size>")

    node.query("SYSTEM RELOAD CONFIG")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096, -1)}")


# NOTE: this test takes a couple of minutes when run together with other tests
@pytest.mark.long_run
def test_restart_during_load(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    # Force multi-part upload mode.
    replace_config("<container_already_exists>false</container_already_exists>", "")

    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 1024 * 1024)}")
    node.query(f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 1024 * 1024, -1)}")


    def read():
        for ii in range(0, 5):
            logging.info(f"Executing {ii} query")
            assert node.query(f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
            logging.info(f"Query {ii} executed")
            time.sleep(0.2)

    def restart_disk():
        for iii in range(0, 2):
            logging.info(f"Restarting disk, attempt {iii}")
            node.query(f"SYSTEM RESTART DISK {BLOB_STORAGE_DISK}")
            logging.info(f"Disk restarted, attempt {iii}")
            time.sleep(0.5)

    threads = []
    for _ in range(0, 4):
        threads.append(SafeThread(target=read))

    threads.append(SafeThread(target=restart_disk))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
