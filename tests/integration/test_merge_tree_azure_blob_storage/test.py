import logging
import time
import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.utility import generate_values, replace_config, SafeThread


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NODE_NAME = "node"
TABLE_NAME = "blob_storage_table"
AZURE_BLOB_STORAGE_DISK = "blob_storage_disk"
LOCAL_DISK = "hdd"
CONTAINER_NAME = "cont"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            NODE_NAME,
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/bg_processing_pool_conf.xml",
            ],
            with_azurite=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


# Note: use this for selects and inserts and create table queries.
# For inserts there is no guarantee that retries will not result in duplicates.
# But it is better to retry anyway because 'Connection was closed by the server' error
# happens in fact only for inserts because reads already have build-in retries in code.
def azure_query(node, query, try_num=3):
    for i in range(try_num):
        try:
            return node.query(query)
        except Exception as ex:
            retriable_errors = [
                "DB::Exception: Azure::Core::Http::TransportException: Connection was closed by the server while trying to read a response"
            ]
            retry = False
            for error in retriable_errors:
                if error in str(ex):
                    retry = True
                    logging.info(f"Try num: {i}. Having retriable error: {ex}")
                    break
            if not retry or i == try_num - 1:
                raise Exception(ex)
            continue


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "old_parts_lifetime": 1,
        "index_granularity": 512,
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
    azure_query(node, create_table_statement)
    assert (
        azure_query(node, f"SELECT COUNT(*) FROM {table_name} FORMAT Values") == "(0)"
    )


def test_create_table(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)


def test_read_after_cache_is_wiped(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    values = "('2021-11-13',3,'hello'),('2021-11-14',4,'heyo')"

    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values}")

    # Wipe cache
    cluster.exec_in_container(
        cluster.get_container_id(NODE_NAME),
        ["rm", "-rf", "/var/lib/clickhouse/disks/blob_storage_disk/cache/"],
    )

    # After cache is populated again, only .bin files should be accessed from Blob Storage.
    assert (
        azure_query(node, f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values")
        == values
    )


def test_simple_insert_select(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    values = "('2021-11-13',3,'hello')"
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values}")
    assert (
        azure_query(node, f"SELECT dt, id, data FROM {TABLE_NAME} FORMAT Values")
        == values
    )
    blob_container_client = cluster.blob_service_client.get_container_client(
        CONTAINER_NAME
    )
    assert (
        len(list(blob_container_client.list_blobs())) >= 12
    )  # 1 format file + 2 skip index files + 9 regular MergeTree files + leftovers from other tests


def test_inserts_selects(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    values1 = generate_values("2020-01-03", 4096)
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values1}")
    assert (
        azure_query(node, f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values")
        == values1
    )

    values2 = generate_values("2020-01-04", 4096)
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values2}")
    assert (
        azure_query(node, f"SELECT * FROM {TABLE_NAME} ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )

    assert (
        azure_query(
            node, f"SELECT count(*) FROM {TABLE_NAME} where id = 1 FORMAT Values"
        )
        == "(2)"
    )


@pytest.mark.parametrize("merge_vertical", [(True), (False)])
def test_insert_same_partition_and_merge(cluster, merge_vertical):
    settings = {}
    if merge_vertical:
        settings["vertical_merge_algorithm_min_rows_to_activate"] = 0
        settings["vertical_merge_algorithm_min_columns_to_activate"] = 0

    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME, **settings)

    node.query(f"SYSTEM STOP MERGES {TABLE_NAME}")
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 1024)}"
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 2048)}"
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 1024, -1)}",
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 2048, -1)}",
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096, -1)}",
    )
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(distinct(id)) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(f"SYSTEM START MERGES {TABLE_NAME}")

    # Wait for merges and old parts deletion
    for attempt in range(0, 10):
        parts_count = azure_query(
            node,
            f"SELECT COUNT(*) FROM system.parts WHERE table = '{TABLE_NAME}' FORMAT Values",
        )
        if parts_count == "(1)":
            break

        if attempt == 9:
            assert parts_count == "(1)"

        time.sleep(1)

    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(distinct(id)) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )


def test_alter_table_columns(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096, -1)}",
    )

    node.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have been merged
    node.query(f"OPTIMIZE TABLE {TABLE_NAME}")

    assert (
        azure_query(node, f"SELECT sum(col1) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )
    assert (
        azure_query(
            node, f"SELECT sum(col1) FROM {TABLE_NAME} WHERE id > 0 FORMAT Values"
        )
        == "(4096)"
    )

    node.query(
        f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN col1 String",
        settings={"mutations_sync": 2},
    )

    assert (
        azure_query(node, f"SELECT distinct(col1) FROM {TABLE_NAME} FORMAT Values")
        == "('1')"
    )


def test_attach_detach_partition(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(4096)"
    )

    node.query(f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(f"ALTER TABLE {TABLE_NAME} DROP PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(4096)"
    )

    node.query(f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-04'")
    node.query(
        f"ALTER TABLE {TABLE_NAME} DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    )


def test_move_partition_to_another_disk(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{LOCAL_DISK}'"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{AZURE_BLOB_STORAGE_DISK}'"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )


def test_table_manipulations(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    renamed_table = TABLE_NAME + "_renamed"

    node.query_with_retry(
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    node.query_with_retry(
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )

    node.query(f"RENAME TABLE {TABLE_NAME} TO {renamed_table}")
    assert (
        azure_query(node, f"SELECT count(*) FROM {renamed_table} FORMAT Values")
        == "(8192)"
    )

    node.query(f"RENAME TABLE {renamed_table} TO {TABLE_NAME}")
    assert node.query(f"CHECK TABLE {TABLE_NAME} FORMAT Values") == "(1)"

    node.query(f"DETACH TABLE {TABLE_NAME}")
    node.query(f"ATTACH TABLE {TABLE_NAME}")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    node.query(f"TRUNCATE TABLE {TABLE_NAME}")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    )


@pytest.mark.long_run
def test_move_replace_partition_to_another_table(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    table_clone_name = TABLE_NAME + "_clone"

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 256)}"
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 256)}"
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 256, -1)}",
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-06', 256, -1)}",
    )
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(1024)"
    )

    create_table(node, table_clone_name)

    node.query(
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-03' TO TABLE {table_clone_name}"
    )
    node.query(
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-05' TO TABLE {table_clone_name}"
    )
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values") == "(512)"
    )
    assert (
        azure_query(node, f"SELECT sum(id) FROM {table_clone_name} FORMAT Values")
        == "(0)"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {table_clone_name} FORMAT Values")
        == "(512)"
    )

    # Add new partitions to source table, but with different values and replace them from copied table.
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 256, -1)}",
    )
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 256)}"
    )
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(1024)"
    )

    node.query(
        f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-03' FROM {table_clone_name}"
    )
    node.query(
        f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-05' FROM {table_clone_name}"
    )
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(1024)"
    )
    assert (
        azure_query(node, f"SELECT sum(id) FROM {table_clone_name} FORMAT Values")
        == "(0)"
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {table_clone_name} FORMAT Values")
        == "(512)"
    )

    node.query(f"DROP TABLE {table_clone_name} NO DELAY")
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(1024)"
    )

    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE")

    node.query(f"DROP TABLE {TABLE_NAME} NO DELAY")


def test_freeze_unfreeze(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    backup1 = "backup1"
    backup2 = "backup2"

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup1}'")
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )
    node.query(f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup2}'")

    azure_query(node, f"TRUNCATE TABLE {TABLE_NAME}")

    # Unfreeze single partition from backup1.
    node.query(
        f"ALTER TABLE {TABLE_NAME} UNFREEZE PARTITION '2020-01-03' WITH NAME '{backup1}'"
    )
    # Unfreeze all partitions from backup2.
    node.query(f"ALTER TABLE {TABLE_NAME} UNFREEZE WITH NAME '{backup2}'")


def test_apply_new_settings(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)
    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/node/configs/config.d/storage_conf.xml".format(
            cluster.instances_dir_name
        ),
    )

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )

    # Force multi-part upload mode.
    replace_config(
        config_path,
        "<max_single_part_upload_size>33554432</max_single_part_upload_size>",
        "<max_single_part_upload_size>4096</max_single_part_upload_size>",
    )

    node.query("SYSTEM RELOAD CONFIG")
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096, -1)}",
    )


# NOTE: this test takes a couple of minutes when run together with other tests
@pytest.mark.long_run
def test_restart_during_load(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)
    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/node/configs/config.d/storage_conf.xml".format(
            cluster.instances_dir_name
        ),
    )

    # Force multi-part upload mode.
    replace_config(
        config_path, "<container_already_exists>false</container_already_exists>", ""
    )

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-05', 4096, -1)}",
    )

    def read():
        for ii in range(0, 5):
            logging.info(f"Executing {ii} query")
            assert (
                azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values")
                == "(0)"
            )
            logging.info(f"Query {ii} executed")
            time.sleep(0.2)

    def restart_disk():
        for iii in range(0, 2):
            logging.info(f"Restarting disk, attempt {iii}")
            node.query(f"SYSTEM RESTART DISK {AZURE_BLOB_STORAGE_DISK}")
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


def test_big_insert(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)
    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} select '2020-01-03', number, toString(number) from numbers(5000000)",
    )
    assert int(azure_query(node, f"SELECT count() FROM {TABLE_NAME}")) == 5000000
