import logging
import time
import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock, start_mock_servers
from helpers.utility import generate_values, replace_config, SafeThread
from helpers.wait_for_helpers import wait_for_delete_inactive_parts
from helpers.wait_for_helpers import wait_for_delete_empty_parts
from helpers.wait_for_helpers import wait_for_merges


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.xml",
                "configs/config.d/storage_conf.xml",
                "configs/config.d/bg_processing_pool_conf.xml",
            ],
            user_configs=[
                "configs/config.d/users.xml",
            ],
            stay_alive=True,
            with_minio=True,
        )

        cluster.add_instance(
            "node_with_limited_disk",
            main_configs=[
                "configs/config.d/storage_conf.xml",
                "configs/config.d/bg_processing_pool_conf.xml",
            ],
            with_minio=True,
            tmpfs=[
                "/jbod1:size=2M",
            ],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        run_s3_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC = 1
FILES_OVERHEAD_METADATA_VERSION = 1
FILES_OVERHEAD_PER_PART_WIDE = (
    FILES_OVERHEAD_PER_COLUMN * 3
    + 2
    + 6
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
)
FILES_OVERHEAD_PER_PART_COMPACT = (
    10 + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC + FILES_OVERHEAD_METADATA_VERSION
)


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "s3",
        "old_parts_lifetime": 0,
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

    node.query(f"DROP TABLE IF EXISTS {table_name}")
    node.query(create_table_statement)


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8083")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


def run_s3_mocks(cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        cluster,
        script_dir,
        [
            ("unstable_proxy.py", "resolver", "8081"),
            ("no_delete_objects.py", "resolver", "8082"),
        ],
    )


def list_objects(cluster, path="data/", hint="list_objects"):
    minio = cluster.minio_client
    objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    logging.info(f"{hint} ({len(objects)}): {[x.object_name for x in objects]}")
    return objects


def wait_for_delete_s3_objects(cluster, expected, timeout=30):
    while timeout > 0:
        if len(list_objects(cluster, "data/")) == expected:
            return
        timeout -= 1
        time.sleep(1)
    assert len(list_objects(cluster, "data/")) == expected


def remove_all_s3_objects(cluster):
    minio = cluster.minio_client
    for obj in list_objects(cluster, "data/"):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.fixture(autouse=True, scope="function")
def clear_minio(cluster):
    try:
        # CH do some writes to the S3 at start. For example, file data/clickhouse_access_check_{server_uuid}.
        # Set the timeout there as 10 sec in order to resolve the race with that file exists.
        wait_for_delete_s3_objects(cluster, 0, timeout=10)
    except:
        # Remove extra objects to prevent tests cascade failing
        remove_all_s3_objects(cluster)

    yield


def check_no_objects_after_drop(cluster, table_name="s3_test", node_name="node"):
    node = cluster.instances[node_name]
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    wait_for_delete_s3_objects(cluster, 0, timeout=0)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part,node_name",
    [
        (0, FILES_OVERHEAD_PER_PART_WIDE, "node"),
        (8192, FILES_OVERHEAD_PER_PART_COMPACT, "node"),
    ],
)
def test_simple_insert_select(
    cluster, min_rows_for_wide_part, files_per_part, node_name
):
    node = cluster.instances[node_name]
    create_table(node, "s3_test", min_rows_for_wide_part=min_rows_for_wide_part)
    minio = cluster.minio_client

    values1 = generate_values("2020-01-03", 4096)
    node.query("INSERT INTO s3_test VALUES {}".format(values1))
    assert node.query("SELECT * FROM s3_test order by dt, id FORMAT Values") == values1
    assert len(list_objects(cluster, "data/")) == FILES_OVERHEAD + files_per_part

    values2 = generate_values("2020-01-04", 4096)
    node.query("INSERT INTO s3_test VALUES {}".format(values2))
    assert (
        node.query("SELECT * FROM s3_test ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )
    assert len(list_objects(cluster, "data/")) == FILES_OVERHEAD + files_per_part * 2

    assert (
        node.query("SELECT count(*) FROM s3_test where id = 1 FORMAT Values") == "(2)"
    )

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("merge_vertical,node_name", [(True, "node"), (False, "node")])
def test_insert_same_partition_and_merge(cluster, merge_vertical, node_name):
    settings = {}
    if merge_vertical:
        settings["vertical_merge_algorithm_min_rows_to_activate"] = 0
        settings["vertical_merge_algorithm_min_columns_to_activate"] = 0

    node = cluster.instances[node_name]
    create_table(node, "s3_test", **settings)

    node.query("SYSTEM STOP MERGES s3_test")
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 1024))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 2048))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 1024, -1))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 2048, -1))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096, -1))
    )
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert (
        node.query("SELECT count(distinct(id)) FROM s3_test FORMAT Values") == "(8192)"
    )
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD_PER_PART_WIDE * 6 + FILES_OVERHEAD
    )

    node.query("SYSTEM START MERGES s3_test")

    # Wait for merges and old parts deletion
    for attempt in range(0, 60):
        parts_count = node.query(
            "SELECT COUNT(*) FROM system.parts WHERE table = 's3_test' and active = 1 FORMAT Values"
        )

        if parts_count == "(1)":
            break

        if attempt == 59:
            assert parts_count == "(1)"

        time.sleep(1)

    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert (
        node.query("SELECT count(distinct(id)) FROM s3_test FORMAT Values") == "(8192)"
    )
    wait_for_delete_s3_objects(
        cluster, FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD, timeout=45
    )

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_alter_table_columns(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096, -1))
    )

    node.query("ALTER TABLE s3_test ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3_test")

    assert node.query("SELECT sum(col1) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        node.query("SELECT sum(col1) FROM s3_test WHERE id > 0 FORMAT Values")
        == "(4096)"
    )
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN,
    )

    node.query(
        "ALTER TABLE s3_test MODIFY COLUMN col1 String", settings={"mutations_sync": 2}
    )

    assert node.query("SELECT distinct(col1) FROM s3_test FORMAT Values") == "('1')"
    # and file with mutation
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN + 1,
    )

    node.query("ALTER TABLE s3_test DROP COLUMN col1", settings={"mutations_sync": 2})

    # and 2 files with mutations
    wait_for_delete_s3_objects(
        cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + 2
    )

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_attach_detach_partition(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-03'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 2
        - FILES_OVERHEAD_METADATA_VERSION
    )

    node.query("ALTER TABLE s3_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("ALTER TABLE s3_test DROP PARTITION '2020-01-03'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 1
    )

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-04'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 1
        - FILES_OVERHEAD_METADATA_VERSION
    )
    node.query(
        "ALTER TABLE s3_test DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 0
    )

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_move_partition_to_another_disk(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE
    )

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-04' TO DISK 's3'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_table_manipulations(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )

    node.query("RENAME TABLE s3_test TO s3_renamed")
    assert node.query("SELECT count(*) FROM s3_renamed FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("RENAME TABLE s3_renamed TO s3_test")

    assert node.query("CHECK TABLE s3_test FORMAT Values") == "(1)"

    node.query("DETACH TABLE s3_test")
    node.query("ATTACH TABLE s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2
    )

    node.query("TRUNCATE TABLE s3_test")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    assert len(list_objects(cluster, "data/")) == FILES_OVERHEAD

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_move_replace_partition_to_another_table(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-05", 4096, -1))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-06", 4096, -1))
    )
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"

    assert (
        len(list_objects(cluster, "data/", "Objects at start"))
        == FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4
    )
    create_table(node, "s3_clone")

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-03' TO TABLE s3_clone")
    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-05' TO TABLE s3_clone")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    assert node.query("SELECT sum(id) FROM s3_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_clone FORMAT Values") == "(8192)"

    list_objects(cluster, "data/", "Object after move partition")
    # Number of objects in S3 should be unchanged.
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    # Add new partitions to source table, but with different values and replace them from copied table.
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096, -1))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"

    list_objects(cluster, "data/", "Object after insert")
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 6
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("ALTER TABLE s3_test REPLACE PARTITION '2020-01-03' FROM s3_clone")
    node.query("ALTER TABLE s3_test REPLACE PARTITION '2020-01-05' FROM s3_clone")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"
    assert node.query("SELECT sum(id) FROM s3_clone FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_clone FORMAT Values") == "(8192)"

    # Wait for outdated partitions deletion.
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD * 2
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("DROP TABLE s3_clone SYNC")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"

    list_objects(cluster, "data/", "Object after drop")
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("ALTER TABLE s3_test FREEZE")
    # Number S3 objects should be unchanged.
    list_objects(cluster, "data/", "Object after freeze")
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2,
    )

    node.query("DROP TABLE s3_test SYNC")
    # Backup data should remain in S3.

    wait_for_delete_s3_objects(
        cluster, FILES_OVERHEAD_PER_PART_WIDE * 4 - FILES_OVERHEAD_METADATA_VERSION * 4
    )

    remove_all_s3_objects(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_freeze_unfreeze(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query("ALTER TABLE s3_test FREEZE WITH NAME 'backup1'")
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query("ALTER TABLE s3_test FREEZE WITH NAME 'backup2'")

    node.query("TRUNCATE TABLE s3_test")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD
        + (FILES_OVERHEAD_PER_PART_WIDE - FILES_OVERHEAD_METADATA_VERSION) * 2
    )

    # Unfreeze single partition from backup1.
    node.query(
        "ALTER TABLE s3_test UNFREEZE PARTITION '2020-01-03' WITH NAME 'backup1'"
    )
    # Unfreeze all partitions from backup2.
    node.query("ALTER TABLE s3_test UNFREEZE WITH NAME 'backup2'")

    # Data should be removed from S3.
    wait_for_delete_s3_objects(cluster, FILES_OVERHEAD)

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_freeze_system_unfreeze(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    create_table(node, "s3_test_removed")

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    node.query("ALTER TABLE s3_test FREEZE WITH NAME 'backup3'")
    node.query("ALTER TABLE s3_test_removed FREEZE WITH NAME 'backup3'")

    node.query("TRUNCATE TABLE s3_test")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    node.query("DROP TABLE s3_test_removed SYNC")
    assert (
        len(list_objects(cluster, "data/"))
        == FILES_OVERHEAD
        + (FILES_OVERHEAD_PER_PART_WIDE - FILES_OVERHEAD_METADATA_VERSION) * 2
    )

    # Unfreeze all data from backup3.
    node.query("SYSTEM UNFREEZE WITH NAME 'backup3'")

    # Data should be removed from S3.
    wait_for_delete_s3_objects(cluster, FILES_OVERHEAD)

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_s3_disk_apply_new_settings(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")

    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/node/configs/config.d/storage_conf.xml".format(
            cluster.instances_dir_name
        ),
    )

    def get_s3_requests():
        node.query("SYSTEM FLUSH LOGS")
        return int(
            node.query(
                "SELECT value FROM system.events WHERE event='S3WriteRequestsCount'"
            )
        )

    s3_requests_before = get_s3_requests()
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    s3_requests_to_write_partition = get_s3_requests() - s3_requests_before

    # Force multi-part upload mode.
    replace_config(
        config_path,
        "<s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>",
        "<s3_max_single_part_upload_size>0</s3_max_single_part_upload_size>",
    )

    node.query("SYSTEM RELOAD CONFIG")

    s3_requests_before = get_s3_requests()
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )

    # There should be 3 times more S3 requests because multi-part upload mode uses 3 requests to upload object.
    assert get_s3_requests() - s3_requests_before == s3_requests_to_write_partition * 3

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_s3_no_delete_objects(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(
        node, "s3_test_no_delete_objects", storage_policy="no_delete_objects_s3"
    )
    node.query("DROP TABLE s3_test_no_delete_objects SYNC")
    remove_all_s3_objects(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_s3_disk_reads_on_unstable_connection(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test", storage_policy="unstable_s3")
    node.query(
        "INSERT INTO s3_test SELECT today(), *, toString(*) FROM system.numbers LIMIT 9000000"
    )
    for i in range(30):
        print(f"Read sequence {i}")
        assert node.query("SELECT sum(id) FROM s3_test").splitlines() == [
            "40499995500000"
        ]
    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_lazy_seek_optimization_for_async_read(cluster, node_name):
    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3';"
    )
    node.query("SYSTEM STOP MERGES s3_test")
    node.query(
        "INSERT INTO s3_test SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000000"
    )
    node.query("SELECT * FROM s3_test WHERE value LIKE '%abc%' ORDER BY value LIMIT 10")

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node_with_limited_disk"])
def test_cache_with_full_disk_space(cluster, node_name):
    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test (key UInt32, value String) Engine=MergeTree() ORDER BY value SETTINGS storage_policy='s3_with_cache_and_jbod';"
    )
    node.query("SYSTEM STOP MERGES s3_test")
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(100000000)"
    )
    out = node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "benchmark",
            "--iterations",
            "10",
            "--max_threads",
            "100",
            "--query",
            "SELECT count() FROM s3_test WHERE key < 40000000 or key > 80000000 SETTINGS max_read_buffer_size='44Ki'",
        ]
    )
    assert node.contains_in_log(
        "Insert into cache is skipped due to insufficient disk space"
    )
    check_no_objects_after_drop(cluster, node_name=node_name)


@pytest.mark.parametrize("node_name", ["node"])
def test_merge_canceled_by_drop(cluster, node_name):
    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS test_merge_canceled_by_drop NO DELAY")
    node.query(
        "CREATE TABLE test_merge_canceled_by_drop "
        " (key UInt32, value String)"
        " Engine=MergeTree() "
        " ORDER BY value "
        " SETTINGS storage_policy='s3'"
    )
    node.query("SYSTEM STOP MERGES test_merge_canceled_by_drop")
    node.query(
        "INSERT INTO test_merge_canceled_by_drop SELECT number, toString(number) FROM numbers(100000000)"
    )
    node.query("SYSTEM START MERGES test_merge_canceled_by_drop")

    wait_for_merges(node, "test_merge_canceled_by_drop")
    check_no_objects_after_drop(
        cluster, table_name="test_merge_canceled_by_drop", node_name=node_name
    )


@pytest.mark.parametrize("storage_policy", ["broken_s3_always_multi_part", "broken_s3"])
@pytest.mark.parametrize("node_name", ["node"])
def test_merge_canceled_by_s3_errors(cluster, broken_s3, node_name, storage_policy):
    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS test_merge_canceled_by_s3_errors NO DELAY")
    node.query(
        "CREATE TABLE test_merge_canceled_by_s3_errors "
        " (key UInt32, value String)"
        " Engine=MergeTree() "
        " ORDER BY value "
        f" SETTINGS storage_policy='{storage_policy}'"
    )
    node.query("SYSTEM STOP MERGES test_merge_canceled_by_s3_errors")
    node.query(
        "INSERT INTO test_merge_canceled_by_s3_errors SELECT number, toString(number) FROM numbers(10000)"
    )
    node.query(
        "INSERT INTO test_merge_canceled_by_s3_errors SELECT 2*number, toString(number) FROM numbers(10000)"
    )
    min_key = node.query("SELECT min(key) FROM test_merge_canceled_by_s3_errors")
    assert int(min_key) == 0, min_key

    broken_s3.setup_error_at_object_upload()
    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_error_at_part_upload()

    node.query("SYSTEM START MERGES test_merge_canceled_by_s3_errors")

    error = node.query_and_get_error(
        "OPTIMIZE TABLE test_merge_canceled_by_s3_errors FINAL",
    )
    assert "ExpectedError Message: mock s3 injected error" in error, error

    node.wait_for_log_line("ExpectedError Message: mock s3 injected error")

    check_no_objects_after_drop(
        cluster, table_name="test_merge_canceled_by_s3_errors", node_name=node_name
    )


@pytest.mark.parametrize("node_name", ["node"])
def test_merge_canceled_by_s3_errors_when_move(cluster, broken_s3, node_name):
    node = cluster.instances[node_name]
    settings = {
        "storage_policy": "external_broken_s3",
        "merge_with_ttl_timeout": 1,
    }
    create_table(node, "merge_canceled_by_s3_errors_when_move", **settings)

    node.query("SYSTEM STOP MERGES merge_canceled_by_s3_errors_when_move")
    node.query(
        "INSERT INTO merge_canceled_by_s3_errors_when_move"
        " VALUES {}".format(generate_values("2020-01-03", 1000))
    )
    node.query(
        "INSERT INTO merge_canceled_by_s3_errors_when_move"
        " VALUES {}".format(generate_values("2020-01-03", 1000, -1))
    )

    node.query(
        "ALTER TABLE merge_canceled_by_s3_errors_when_move"
        "    MODIFY TTL"
        "        dt + INTERVAL 1 DAY "
        "        TO VOLUME 'external'",
        settings={"materialize_ttl_after_modify": 0},
    )

    broken_s3.setup_error_at_object_upload(count=1, after=1)

    node.query("SYSTEM START MERGES merge_canceled_by_s3_errors_when_move")

    node.query("OPTIMIZE TABLE merge_canceled_by_s3_errors_when_move FINAL")

    node.wait_for_log_line("ExpectedError Message: mock s3 injected error")

    count = node.query("SELECT count() FROM merge_canceled_by_s3_errors_when_move")
    assert int(count) == 2000, count

    check_no_objects_after_drop(
        cluster, table_name="merge_canceled_by_s3_errors_when_move", node_name=node_name
    )


@pytest.mark.parametrize("node_name", ["node"])
@pytest.mark.parametrize(
    "in_flight_memory", [(10, 245918115), (5, 156786752), (1, 106426187)]
)
def test_s3_engine_heavy_write_check_mem(
    cluster, broken_s3, node_name, in_flight_memory
):
    in_flight = in_flight_memory[0]
    memory = in_flight_memory[1]

    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test"
        " ("
        "   key UInt32 CODEC(NONE), value String CODEC(NONE)"
        " )"
        " ENGINE S3('http://resolver:8083/root/data/test-upload.csv', 'minio', 'minio123', 'CSV')",
    )

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_slow_answers(10 * 1024 * 1024, timeout=15, count=10)

    query_id = f"INSERT_INTO_S3_ENGINE_QUERY_ID_{in_flight}"
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(50000000)"
        f" SETTINGS max_memory_usage={2*memory}"
        f", s3_max_inflight_parts_for_one_file={in_flight}",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")

    memory_usage, wait_inflight = node.query(
        "SELECT memory_usage, ProfileEvents['WriteBufferFromS3WaitInflightLimitMicroseconds']"
        " FROM system.query_log"
        f" WHERE query_id='{query_id}'"
        "   AND type!='QueryStart'"
    ).split()

    assert int(memory_usage) < 1.2 * memory
    assert int(memory_usage) > 0.8 * memory

    assert int(wait_inflight) > 10 * 1000 * 1000

    check_no_objects_after_drop(cluster, node_name=node_name)


@pytest.mark.parametrize("node_name", ["node"])
def test_s3_disk_heavy_write_check_mem(cluster, broken_s3, node_name):
    memory = 2279055040

    node = cluster.instances[node_name]
    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test"
        " ("
        "   key UInt32, value String"
        " )"
        " ENGINE=MergeTree()"
        " ORDER BY key"
        " SETTINGS"
        " storage_policy='broken_s3'",
    )
    node.query("SYSTEM STOP MERGES s3_test")

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_slow_answers(10 * 1024 * 1024, timeout=10, count=50)

    query_id = f"INSERT_INTO_S3_DISK_QUERY_ID"
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(50000000)"
        f" SETTINGS max_memory_usage={2*memory}"
        f", max_insert_block_size=50000000"
        f", min_insert_block_size_rows=50000000"
        f", min_insert_block_size_bytes=1000000000000",
        query_id=query_id,
    )

    node.query("SYSTEM FLUSH LOGS")

    result = node.query(
        "SELECT memory_usage"
        " FROM system.query_log"
        f" WHERE query_id='{query_id}'"
        "   AND type!='QueryStart'"
    )

    assert int(result) < 1.2 * memory
    assert int(result) > 0.8 * memory

    check_no_objects_after_drop(cluster, node_name=node_name)


def get_memory_usage(node, query_id):
    node.query("SYSTEM FLUSH LOGS")
    memory_usage = node.query(
        "SELECT memory_usage"
        " FROM system.query_log"
        f" WHERE query_id='{query_id}'"
        "   AND type='QueryFinish'"
    )
    return int(memory_usage)


def get_memory_usages(node, query_ids):
    node.query("SYSTEM FLUSH LOGS")
    result = []
    for query_id in query_ids:
        memory_usage = node.query(
            "SELECT memory_usage"
            " FROM system.query_log"
            f" WHERE query_id='{query_id}'"
            "   AND type='QueryFinish'"
        )
        result.append(int(memory_usage))
    return result


@pytest.mark.parametrize("node_name", ["node"])
def test_heavy_insert_select_check_memory(cluster, broken_s3, node_name):
    node = cluster.instances[node_name]

    node.query(
        """
        CREATE TABLE central_query_log
        (
            control_plane_id UUID,
            pod_id LowCardinality(String),
            scrape_ts_microseconds DateTime64(6) CODEC(Delta(8), LZ4),
            event_date Date,
            event_time DateTime,
            payload Array(String),
            payload_01 String,
            payload_02 String,
            payload_03 String,
            payload_04 String,
            payload_05 String,
            payload_06 String,
            payload_07 String,
            payload_08 String,
            payload_09 String,
            payload_10 String,
            payload_11 String,
            payload_12 String,
            payload_13 String,
            payload_14 String,
            payload_15 String,
            payload_16 String,
            payload_17 String,
            payload_18 String,
            payload_19 String
        )
        ENGINE=MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (control_plane_id, event_date, pod_id)
        SETTINGS
            storage_policy='s3'
        """
    )

    node.query("SYSTEM STOP MERGES central_query_log")

    write_count = 2
    write_query_ids = []
    for x in range(write_count):
        query_id = f"INSERT_INTO_TABLE_RANDOM_DATA_QUERY_ID_{x}"
        write_query_ids.append(query_id)
        node.query(
            """
            INSERT INTO central_query_log
            SELECT
                control_plane_id,
                pod_id,
                toStartOfHour(event_time) + toIntervalSecond(randUniform(0,60)) as scrape_ts_microseconds,
                toDate(event_time) as event_date,
                event_time,
                payload,
                payload[1] as payload_01,
                payload[2] as payload_02,
                payload[3] as payload_03,
                payload[4] as payload_04,
                payload[5] as payload_05,
                payload[6] as payload_06,
                payload[7] as payload_07,
                payload[8] as payload_08,
                payload[9] as payload_09,
                payload[10] as payload_10,
                payload[11] as payload_11,
                payload[12] as payload_12,
                payload[13] as payload_13,
                payload[14] as payload_14,
                payload[15] as payload_15,
                payload[16] as payload_16,
                payload[17] as payload_17,
                payload[18] as payload_18,
                payload[19] as payload_19
            FROM
            (
                SELECT
                    control_plane_id,
                    substring(payload[1], 1, 5) as pod_id,
                    toDateTime('2022-12-12 00:00:00')
                        + toIntervalDay(floor(randUniform(0,3)))
                        + toIntervalHour(floor(randUniform(0,24)))
                        + toIntervalSecond(floor(randUniform(0,60)))
                        as event_time,
                    payload
                FROM
                    generateRandom(
                        'control_plane_id UUID, payload Array(String)',
                        NULL,
                        100,
                        100
                    )
                LIMIT 10000
            )
            SETTINGS
                max_insert_block_size=256000000,
                min_insert_block_size_rows=1000000,
                min_insert_block_size_bytes=256000000
            """,
            query_id=query_id,
        )

    memory = 845346116
    for memory_usage, query_id in zip(
        get_memory_usages(node, write_query_ids), write_query_ids
    ):
        assert int(memory_usage) < 1.2 * memory, f"{memory_usage} : {query_id}"
        assert int(memory_usage) > 0.8 * memory, f"{memory_usage} : {query_id}"

    broken_s3.setup_slow_answers(minimal_length=1000, timeout=5, count=20)
    broken_s3.setup_fake_multpartuploads()

    insert_query_id = f"INSERT_INTO_S3_FUNCTION_QUERY_ID"
    node.query(
        """
        INSERT INTO
            TABLE FUNCTION s3(
                'http://resolver:8083/root/data/test-upload_{_partition_id}.csv.gz',
                'minio', 'minio123',
                'CSV', auto, 'gzip'
            )
        PARTITION BY formatDateTime(subtractHours(toDateTime('2022-12-13 00:00:00'), 1),'%Y-%m-%d_%H:00')
        WITH toDateTime('2022-12-13 00:00:00') as time_point
        SELECT
            *
        FROM central_query_log
        WHERE
            event_date >= subtractDays(toDate(time_point), 1)
            AND scrape_ts_microseconds >= subtractHours(toStartOfHour(time_point), 12)
            AND scrape_ts_microseconds < toStartOfDay(time_point)
        SETTINGS
            s3_max_inflight_parts_for_one_file=1
        """,
        query_id=insert_query_id,
    )

    query_id = f"SELECT_QUERY_ID"
    total = node.query(
        """
        SELECT
            count()
        FROM central_query_log
        """,
        query_id=query_id,
    )
    assert int(total) == 10000 * write_count

    query_id = f"SELECT_WHERE_QUERY_ID"
    selected = node.query(
        """
        WITH toDateTime('2022-12-13 00:00:00') as time_point
        SELECT
            count()
        FROM central_query_log
        WHERE
            event_date >= subtractDays(toDate(time_point), 1)
            AND scrape_ts_microseconds >= subtractHours(toStartOfHour(time_point), 12)
            AND scrape_ts_microseconds < toStartOfDay(time_point)
        """,
        query_id=query_id,
    )
    assert int(selected) < 4500, selected
    assert int(selected) > 2500, selected

    node.query("SYSTEM FLUSH LOGS")
    profile_events = node.query(
        f"""
        SELECT ProfileEvents
            FROM system.query_log
            WHERE query_id='{insert_query_id}'
            AND type='QueryFinish'
        """
    )

    memory_usage = get_memory_usage(node, insert_query_id)
    memory = 123507857
    assert int(memory_usage) < 1.2 * memory, f"{memory_usage} {profile_events}"
    assert int(memory_usage) > 0.8 * memory, f"{memory_usage} {profile_events}"

    node.query(f"DROP TABLE IF EXISTS central_query_log SYNC")
    remove_all_s3_objects(cluster)
