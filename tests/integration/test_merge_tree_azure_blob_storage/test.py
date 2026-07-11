import logging
import os
import time

import pytest
from azure.storage.blob import BlobServiceClient

from helpers.cluster import ClickHouseCluster
from helpers.utility import SafeThread, generate_values, replace_config
from test_storage_azure_blob_storage.test import azure_query

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
NODE_NAME = "node"
TABLE_NAME = "blob_storage_table"
AZURE_BLOB_STORAGE_DISK = "blob_storage_disk"
LOCAL_DISK = "hdd"
CONTAINER_NAME = "cont"


def generate_cluster_def(port):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/disk_storage_conf.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""<clickhouse>
    <storage_configuration>
        <disks>
            <blob_storage_disk>
                <type>azure_blob_storage</type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>cont</container_name>
                <skip_access_check>false</skip_access_check>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
                <max_single_part_upload_size>100000</max_single_part_upload_size>
                <min_upload_part_size>100000</min_upload_part_size>
                <max_single_download_retries>10</max_single_download_retries>
                <max_single_read_retries>10</max_single_read_retries>
            </blob_storage_disk>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <blob_storage_policy>
                <volumes>
                    <main>
                        <disk>blob_storage_disk</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </blob_storage_policy>
        </policies>
    </storage_configuration>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        path = generate_cluster_def(port)
        cluster.add_instance(
            NODE_NAME,
            main_configs=[
                "configs/config.d/bg_processing_pool_conf.xml",
                path,
            ],
            with_azurite=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


# Note: use azure_query for selects and inserts and create table queries.
# For inserts there is no guarantee that retries will not result in duplicates.
# But it is better to retry anyway because connection related errors
# happens in fact only for inserts because reads already have build-in retries in code.


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "blob_storage_policy",
        "old_parts_lifetime": 1,
        "index_granularity": 512,
        "temporary_directories_lifetime": 1,
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

    azure_query(node, f"DROP TABLE IF EXISTS {table_name}")
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

    # We insert into different partitions, so do it separately to avoid
    # test flakyness when retrying the query in case of retriable exception.
    values = "('2021-11-13',3,'hello')"
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values}")
    values = "('2021-11-14',4,'heyo')"
    azure_query(node, f"INSERT INTO {TABLE_NAME} VALUES {values}")

    # Wipe cache
    cluster.exec_in_container(
        cluster.get_container_id(NODE_NAME),
        ["rm", "-rf", "/var/lib/clickhouse/disks/blob_storage_disk/cache/"],
    )

    # After cache is populated again, only .bin files should be accessed from Blob Storage.
    assert (
        azure_query(
            node, f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values"
        ).strip()
        == "('2021-11-13',3,'hello'),('2021-11-14',4,'heyo')"
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
    for attempt in range(0, 60):
        parts_count = azure_query(
            node,
            f"SELECT COUNT(*) FROM system.parts WHERE table = '{TABLE_NAME}' FORMAT Values",
        )
        if parts_count == "(1)":
            break

        if attempt == 59:
            assert parts_count == "(1)"

        time.sleep(10)

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

    azure_query(node, f"ALTER TABLE {TABLE_NAME} ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have been merged
    azure_query(node, f"OPTIMIZE TABLE {TABLE_NAME}")

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

    azure_query(
        node,
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

    azure_query(node, f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(4096)"
    )

    azure_query(node, f"ALTER TABLE {TABLE_NAME} ATTACH PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    azure_query(node, f"ALTER TABLE {TABLE_NAME} DROP PARTITION '2020-01-03'")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(4096)"
    )

    azure_query(node, f"ALTER TABLE {TABLE_NAME} DETACH PARTITION '2020-01-04'")
    azure_query(
        node,
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

    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{LOCAL_DISK}'",
    )
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-04' TO DISK '{AZURE_BLOB_STORAGE_DISK}'",
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

    azure_query(node, f"RENAME TABLE {TABLE_NAME} TO {renamed_table}")
    assert (
        azure_query(node, f"SELECT count(*) FROM {renamed_table} FORMAT Values")
        == "(8192)"
    )

    azure_query(node, f"RENAME TABLE {renamed_table} TO {TABLE_NAME}")
    assert azure_query(node, f"CHECK TABLE {TABLE_NAME} FORMAT Values") == "(1)"

    node.query(f"DETACH TABLE {TABLE_NAME}")
    node.query(f"ATTACH TABLE {TABLE_NAME}")
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(8192)"
    )

    azure_query(node, f"TRUNCATE TABLE {TABLE_NAME}")
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

    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-03' TO TABLE {table_clone_name}",
    )
    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} MOVE PARTITION '2020-01-05' TO TABLE {table_clone_name}",
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

    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-03' FROM {table_clone_name}",
    )
    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} REPLACE PARTITION '2020-01-05' FROM {table_clone_name}",
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

    azure_query(node, f"DROP TABLE {table_clone_name} SYNC")
    assert azure_query(node, f"SELECT sum(id) FROM {TABLE_NAME} FORMAT Values") == "(0)"
    assert (
        azure_query(node, f"SELECT count(*) FROM {TABLE_NAME} FORMAT Values")
        == "(1024)"
    )

    azure_query(node, f"ALTER TABLE {TABLE_NAME} FREEZE")

    azure_query(node, f"DROP TABLE {TABLE_NAME} SYNC")


def test_freeze_unfreeze(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    backup1 = "backup1"
    backup2 = "backup2"

    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-03', 4096)}"
    )
    azure_query(node, f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup1}'")
    azure_query(
        node, f"INSERT INTO {TABLE_NAME} VALUES {generate_values('2020-01-04', 4096)}"
    )
    azure_query(node, f"ALTER TABLE {TABLE_NAME} FREEZE WITH NAME '{backup2}'")

    azure_query(node, f"TRUNCATE TABLE {TABLE_NAME}")

    # Unfreeze single partition from backup1.
    azure_query(
        node,
        f"ALTER TABLE {TABLE_NAME} UNFREEZE PARTITION '2020-01-03' WITH NAME '{backup1}'",
    )
    # Unfreeze all partitions from backup2.
    azure_query(node, f"ALTER TABLE {TABLE_NAME} UNFREEZE WITH NAME '{backup2}'")


def test_apply_new_settings(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)
    config_path = os.path.join(SCRIPT_DIR, "./_gen/disk_storage_conf.xml")

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


def test_big_insert(cluster):
    node = cluster.instances[NODE_NAME]
    create_table(node, TABLE_NAME)

    check_query = "SELECT '2020-01-03', number, toString(number) FROM numbers(1000000)"

    azure_query(
        node,
        f"INSERT INTO {TABLE_NAME} {check_query}",
    )
    assert azure_query(node, f"SELECT * FROM {TABLE_NAME} ORDER BY id") == azure_query(
        node, check_query
    )

    blob_container_client = cluster.blob_service_client.get_container_client(
        CONTAINER_NAME
    )

    blobs = blob_container_client.list_blobs()
    max_single_part_upload_size = 100000
    checked = False

    for blob in blobs:
        blob_client = cluster.blob_service_client.get_blob_client(
            CONTAINER_NAME, blob.name
        )
        committed, uncommited = blob_client.get_block_list()

        blocks = committed
        last_id = len(blocks)
        id = 1
        if len(blocks) > 1:
            checked = True

        for block in blocks:
            print(f"blob: {blob.name}, block size: {block.size}")
            if id == last_id:
                assert max_single_part_upload_size >= block.size
            else:
                assert max_single_part_upload_size == block.size
            id += 1
    assert checked


def test_endpoint(cluster):
    node = cluster.instances[NODE_NAME]
    account_name = "devstoreaccount1"
    container_name = "cont2"
    data_prefix = "data_prefix"
    port = cluster.azurite_port

    container_client = cluster.blob_service_client.get_container_client(container_name)
    container_client.create_container()

    azure_query(
        node,
        f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}/{account_name}/{container_name}/{data_prefix}',
    endpoint_contains_account_name = 'true',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    container_already_exists = 1,
    skip_access_check = 0);

    INSERT INTO test SELECT number FROM numbers(10);
    """,
    )

    assert 10 == int(node.query("SELECT count() FROM test"))


def test_endpoint_new_container(cluster):
    node = cluster.instances[NODE_NAME]
    account_name = "devstoreaccount1"
    container_name = "cont3"
    data_prefix = "data_prefix"
    port = cluster.azurite_port

    azure_query(
        node,
        f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}/{account_name}/{container_name}/{data_prefix}',
    endpoint_contains_account_name = 'true',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    skip_access_check = 0);

    INSERT INTO test SELECT number FROM numbers(10);
    """,
    )

    assert 10 == int(node.query("SELECT count() FROM test"))


def test_endpoint_without_prefix(cluster):
    node = cluster.instances[NODE_NAME]
    account_name = "devstoreaccount1"
    container_name = "cont4"
    port = cluster.azurite_port

    azure_query(
        node,
        f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}/{account_name}/{container_name}',
    endpoint_contains_account_name = 'true',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    skip_access_check = 0);

    INSERT INTO test SELECT number FROM numbers(10);
    """,
    )

    assert 10 == int(node.query("SELECT count() FROM test"))


def test_endpoint_error_check(cluster):
    node = cluster.instances[NODE_NAME]
    account_name = "devstoreaccount1"
    port = cluster.azurite_port

    query = f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}/{account_name}',
    endpoint_contains_account_name = 'true',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    skip_access_check = 0);
    """

    expected_err_msg = "Expected container_name in endpoint"
    assert expected_err_msg in azure_query(node, query, expect_error=True)

    query = f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}',
    endpoint_contains_account_name = 'true',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    skip_access_check = 0);
    """

    expected_err_msg = "Expected account_name in endpoint"
    assert expected_err_msg in azure_query(node, query, expect_error=True)

    query = f"""
    DROP TABLE IF EXISTS test SYNC;

    CREATE TABLE test (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
    type = azure_blob_storage,
    endpoint = 'http://azurite1:{port}',
    endpoint_contains_account_name = 'false',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    skip_access_check = 0);
    """

    expected_err_msg = "Expected container_name in endpoint"
    assert expected_err_msg in azure_query(node, query, expect_error=True)


def get_azure_client(container_name, port):
    connection_string = (
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        f"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        f"BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
    )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client.get_container_client(container_name)


def test_azure_broken_parts(cluster):
    node = cluster.instances[NODE_NAME]
    account_name = "devstoreaccount1"
    container_name = "cont5"
    port = cluster.azurite_port

    query = f"""
    DROP TABLE IF EXISTS t_azure_broken_parts SYNC;

    CREATE TABLE t_azure_broken_parts (a Int32)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
        type = azure_blob_storage,
        endpoint = 'http://azurite1:{port}/{account_name}/{container_name}',
        endpoint_contains_account_name = 'true',
        account_name = 'devstoreaccount1',
        account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
        skip_access_check = 0), min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

    INSERT INTO t_azure_broken_parts VALUES (1);
    """

    azure_query(node, query)

    result = azure_query(node, "SELECT count() FROM t_azure_broken_parts").strip()
    assert int(result) == 1

    result = azure_query(
        node,
        "SELECT count() FROM system.detached_parts WHERE table = 't_azure_broken_parts'",
    ).strip()

    assert int(result) == 0

    data_path = azure_query(
        node,
        "SELECT data_paths[1] FROM system.tables WHERE name = 't_azure_broken_parts'",
    ).strip()

    remote_path = azure_query(
        node,
        f"SELECT remote_path FROM system.remote_data_paths WHERE path || local_path = '{data_path}' || 'all_1_1_0/columns.txt'",
    ).strip()

    client = get_azure_client(container_name, port)
    client.delete_blob(remote_path)

    azure_query(node, "DETACH TABLE t_azure_broken_parts")
    azure_query(node, "ATTACH TABLE t_azure_broken_parts")

    result = azure_query(node, "SELECT count() FROM t_azure_broken_parts").strip()
    assert int(result) == 0

    result = azure_query(
        node,
        "SELECT count() FROM system.detached_parts WHERE table = 't_azure_broken_parts'",
    ).strip()

    assert int(result) == 1
