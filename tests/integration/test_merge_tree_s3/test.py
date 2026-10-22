import ast
import logging
import os
import time
import uuid

import boto3
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock
from helpers.network import PartitionManager
from helpers.utility import generate_values
from helpers.blobs import wait_blobs_count_synchronization
from helpers.test_tools import assert_eq_with_retry, wait_condition
from helpers.wait_for_helpers import (
    wait_for_delete_empty_parts,
    wait_for_delete_inactive_parts,
    wait_for_merges,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.xml",
                "configs/config.d/bg_processing_pool_conf.xml",
                "configs/config.d/blob_log.xml",
            ],
            user_configs=[
                "configs/config.d/users.xml",
            ],
            stay_alive=True,
            with_minio=True,
            with_zookeeper=True,
        )

        cluster.add_instance(
            "node_with_limited_disk",
            main_configs=[
                "configs/config.d/bg_processing_pool_conf.xml",
                "configs/config.d/blob_log.xml",
            ],
            with_minio=True,
            stay_alive=True,
            tmpfs=[
                "/test_merge_tree_s3_jbod1:size=2M",
            ],
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")
        run_s3_mocks(cluster)

        for _, node in cluster.instances.items():
            node.stop_clickhouse()
            node.copy_file_to_container(
                os.path.join(CONFIG_DIR, "config.d", "storage_conf.xml"),
                "/etc/clickhouse-server/config.d/storage_conf.xml",
            )
            node.start_clickhouse()
        yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC = 1
FILES_OVERHEAD_METADATA_VERSION = 1
FILES_OVERHEAD_COLUMNS_SUBSTREAMS = 1
# The minmax skip index goes into a single skp_idx.packed archive instead of a
# separate .idx2 + .mrk2 pair, because packed_skip_index_max_bytes is 1 MiB by default.
FILES_SAVED_BY_PACKED_SKIP_INDEX = 1
FILES_OVERHEAD_PER_PART_WIDE = (
    FILES_OVERHEAD_PER_COLUMN * 3
    + 2
    + 6
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
    + FILES_OVERHEAD_COLUMNS_SUBSTREAMS
    - FILES_SAVED_BY_PACKED_SKIP_INDEX
)
FILES_OVERHEAD_PER_PART_COMPACT = (
    10
    + FILES_OVERHEAD_DEFAULT_COMPRESSION_CODEC
    + FILES_OVERHEAD_METADATA_VERSION
    + FILES_OVERHEAD_COLUMNS_SUBSTREAMS
    - FILES_SAVED_BY_PACKED_SKIP_INDEX
)
FILES_OVERHEAD_PER_INVALIDATED_COLUMN = 1


def create_table(node, table_name, **additional_settings):
    settings = {
        "storage_policy": "s3",
        "old_parts_lifetime": 0,
        "index_granularity": 512,
        "temporary_directories_lifetime": 1,
        "write_marks_for_substreams_in_compact_parts": 1,
        "cleanup_delay_period": 1,
        "cleanup_delay_period_random_add": 0,
        "cleanup_thread_preferred_points_per_iteration": 0,
        "auto_statistics_types": "",
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
    yield start_s3_mock(cluster, "broken_s3", "8085")


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
        existing_objects = list_objects(cluster, "data/")
        if len(existing_objects) == expected:
            return existing_objects
        timeout -= 1
        time.sleep(1)
    existing_objects = list_objects(cluster, "data/")
    assert len(existing_objects) == expected
    return existing_objects


def remove_all_s3_objects(cluster):
    minio = cluster.minio_client
    objects_to_delete = list_objects(cluster, "data/")
    for obj in objects_to_delete:
        minio.remove_object(cluster.minio_bucket, obj.object_name)
    return objects_to_delete


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
    return wait_for_delete_s3_objects(cluster, 0, timeout=30)


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
    insert_query_id = uuid.uuid4().hex

    node.query(
        "INSERT INTO s3_test VALUES {}".format(values1), query_id=insert_query_id
    )
    assert node.query("SELECT * FROM s3_test order by dt, id FORMAT Values") == values1
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + files_per_part)

    node.query("SYSTEM FLUSH LOGS")
    blob_storage_log = node.query(
        f"SELECT * FROM system.blob_storage_log WHERE query_id = '{insert_query_id}' FORMAT PrettyCompactMonoBlock"
    )

    result = node.query(
        f"""SELECT
            (countIf( (event_type == 'Upload' OR event_type == 'MultiPartUploadWrite') as event_match) as total_events) > 0,
            countIf(event_match AND bucket == 'root') == total_events,
            countIf(event_match AND remote_path != '') == total_events,
            countIf(event_match AND local_path != '') == total_events,
            sumIf(data_size, event_match) > 0
        FROM system.blob_storage_log
        WHERE query_id = '{insert_query_id}' AND error == ''
        """
    )
    assert result == "1\t1\t1\t1\t1\n", blob_storage_log

    values2 = generate_values("2020-01-04", 4096)
    node.query("INSERT INTO s3_test VALUES {}".format(values2))
    assert (
        node.query("SELECT * FROM s3_test ORDER BY dt, id FORMAT Values")
        == values1 + "," + values2
    )
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + files_per_part * 2)

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
    minio = cluster.minio_client

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
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD_PER_PART_WIDE * 6 + FILES_OVERHEAD)

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

    def assert_deleted_in_log(old_objects, new_objects):
        node.query("SYSTEM FLUSH LOGS")

        deleted_objects = set(obj.object_name for obj in old_objects) - set(
            obj.object_name for obj in new_objects
        )
        deleted_in_log = set(
            node.query(
                "SELECT remote_path FROM system.blob_storage_log WHERE error == '' AND event_type == 'Delete'"
            )
            .strip()
            .split()
        )

        # all deleted objects should be in log
        assert all(obj in deleted_in_log for obj in deleted_objects), (
            deleted_objects,
            node.query(
                "SELECT * FROM system.blob_storage_log FORMAT PrettyCompactMonoBlock"
            ),
        )

    objects_before = list_objects(cluster, "data/")

    node.query("ALTER TABLE s3_test ADD COLUMN col1 UInt64 DEFAULT 1")
    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3_test")

    assert node.query("SELECT sum(col1) FROM s3_test FORMAT Values") == "(8192)"
    assert (
        node.query("SELECT sum(col1) FROM s3_test WHERE id > 0 FORMAT Values")
        == "(4096)"
    )

    existing_objects = wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN,
    )

    assert_deleted_in_log(objects_before, existing_objects)
    objects_before = existing_objects

    node.query(
        "ALTER TABLE s3_test MODIFY COLUMN col1 String", settings={"mutations_sync": 2}
    )

    assert node.query("SELECT distinct(col1) FROM s3_test FORMAT Values") == "('1')"
    # and file with mutation
    existing_objects = wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + FILES_OVERHEAD_PER_COLUMN + 1,
    )

    assert_deleted_in_log(objects_before, existing_objects)
    objects_before = existing_objects

    node.query("ALTER TABLE s3_test DROP COLUMN col1", settings={"mutations_sync": 2})

    # and 2 files with mutations
    existing_objects = wait_for_delete_s3_objects(
        cluster, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE + 2
    )
    assert_deleted_in_log(objects_before, existing_objects)
    objects_before = existing_objects

    existing_objects = check_no_objects_after_drop(cluster)

    assert_deleted_in_log(objects_before, existing_objects)
    objects_before = existing_objects


@pytest.mark.parametrize("node_name", ["node"])
def test_attach_detach_partition(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    minio = cluster.minio_client

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-03'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("ALTER TABLE s3_test ATTACH PARTITION '2020-01-03'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2 + FILES_OVERHEAD_PER_INVALIDATED_COLUMN)

    node.query("ALTER TABLE s3_test DROP PARTITION '2020-01-03'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(4096)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 1)

    node.query("ALTER TABLE s3_test DETACH PARTITION '2020-01-04'")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 1)

    node.query(
        "ALTER TABLE s3_test DROP DETACHED PARTITION '2020-01-04'",
        settings={"allow_drop_detached": 1},
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD)

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_move_partition_to_another_disk(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    minio = cluster.minio_client

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-04' TO DISK 'hdd'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE)

    node.query("ALTER TABLE s3_test MOVE PARTITION '2020-01-04' TO DISK 's3'")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_table_manipulations(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    minio = cluster.minio_client

    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096))
    )

    node.query("RENAME TABLE s3_test TO s3_renamed")
    assert node.query("SELECT count(*) FROM s3_renamed FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("RENAME TABLE s3_renamed TO s3_test")

    assert node.query("CHECK TABLE s3_test FORMAT Values SETTINGS check_query_single_value_result = 1") == "(1)"

    node.query("DETACH TABLE s3_test")
    node.query("ATTACH TABLE s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(8192)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 2)

    node.query("TRUNCATE TABLE s3_test")
    wait_for_delete_empty_parts(node, "s3_test")
    wait_for_delete_inactive_parts(node, "s3_test")
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(0)"
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD)

    check_no_objects_after_drop(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_move_replace_partition_to_another_table(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    minio = cluster.minio_client

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

    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + FILES_OVERHEAD_PER_PART_WIDE * 4)
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
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
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
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
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
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 4,
    )

    node.query("DROP TABLE s3_clone SYNC")
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "(0)"
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "(16384)"

    list_objects(cluster, "data/", "Object after drop")
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
    )

    node.query("ALTER TABLE s3_test FREEZE")
    # Number S3 objects should be unchanged.
    list_objects(cluster, "data/", "Object after freeze")
    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD
        + FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 2
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
    )

    node.query("DROP TABLE s3_test SYNC")
    # Backup data should remain in S3.
    # The frozen copies of the two adopted parts keep their `invalidated_system_columns.txt`.

    wait_for_delete_s3_objects(
        cluster,
        FILES_OVERHEAD_PER_PART_WIDE * 4
        - FILES_OVERHEAD_METADATA_VERSION * 4
        + FILES_OVERHEAD_PER_INVALIDATED_COLUMN * 2,
    )

    remove_all_s3_objects(cluster)


@pytest.mark.parametrize("node_name", ["node"])
def test_freeze_unfreeze(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(node, "s3_test")
    minio = cluster.minio_client

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
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + (FILES_OVERHEAD_PER_PART_WIDE - FILES_OVERHEAD_METADATA_VERSION) * 2)

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
    minio = cluster.minio_client

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
    wait_blobs_count_synchronization(minio, FILES_OVERHEAD + (FILES_OVERHEAD_PER_PART_WIDE - FILES_OVERHEAD_METADATA_VERSION) * 2)

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

    with open(config_path, "r") as config_file:
        original_config = config_file.read()

    try:
        # Force multi-part upload mode.
        modified_config = original_config.replace(
            "<s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>",
            "<s3_max_single_part_upload_size>0</s3_max_single_part_upload_size>",
        )
        with open(config_path, "w") as config_file:
            config_file.write(modified_config)

        node.query("SYSTEM RELOAD CONFIG")

        s3_requests_before = get_s3_requests()
        node.query(
            "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
        )

        # There should be 3 times more S3 requests because multi-part upload mode uses 3 requests to upload object.
        assert get_s3_requests() - s3_requests_before == s3_requests_to_write_partition * 3

        check_no_objects_after_drop(cluster)

    finally:
        with open(config_path, "w") as config_file:
            config_file.write(original_config)

        node.query("SYSTEM RELOAD CONFIG")


@pytest.mark.parametrize("node_name", ["node"])
def test_s3_no_delete_objects(cluster, node_name):
    node = cluster.instances[node_name]
    create_table(
        node, "s3_test_no_delete_objects", storage_policy="no_delete_objects_s3"
    )
    check_no_objects_after_drop(cluster, 's3_test_no_delete_objects')


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
    # Create a dummy file of 2M size to fill the disk space of cache disk
    node.exec_in_container(
        [
            "/usr/bin/dd",
            "if=/dev/zero",
            "of=/test_merge_tree_s3_jbod1/dummy",
            "bs=1000",
            "count=2000",
        ]
    )
    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test (key UInt32, value String) Engine=MergeTree() ORDER BY value SETTINGS storage_policy='s3_with_cache_and_jbod';"
    )
    node.query("SYSTEM STOP MERGES s3_test")
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(100000000)"
    )
    node.exec_in_container(
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

    rows_count = node.query("SELECT count(key) FROM test_merge_canceled_by_s3_errors")
    assert int(rows_count) == 20000, rows_count

    broken_s3.setup_at_object_upload()
    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload()

    node.query("SYSTEM START MERGES test_merge_canceled_by_s3_errors")

    error = node.query_and_get_error(
        "OPTIMIZE TABLE test_merge_canceled_by_s3_errors FINAL",
    )
    assert "ExpectedError Message: mock s3 injected unretryable error" in error, error

    table_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'test_merge_canceled_by_s3_errors' LIMIT 1"
    ).strip()

    node.query("SYSTEM FLUSH LOGS")
    error_count_in_blob_log = node.query(
        f"SELECT count() FROM system.blob_storage_log WHERE query_id like '{table_uuid}::%' AND error like '%mock s3 injected unretryable error%'"
    ).strip()
    assert int(error_count_in_blob_log) > 0, node.query(
        f"SELECT * FROM system.blob_storage_log WHERE query_id like '{table_uuid}::%' FORMAT PrettyCompactMonoBlock"
    )

    check_no_objects_after_drop(
        cluster, table_name="test_merge_canceled_by_s3_errors", node_name=node_name
    )


@pytest.fixture
def s3_cancellation_read_mode(request, cluster, broken_s3):
    if not hasattr(request, "param"):
        yield None
        return

    node = cluster.instances["node"]
    config_path = "/etc/clickhouse-server/users.d/users.xml"
    original_config = node.exec_in_container(["cat", config_path])
    assert original_config.count("</profiles>") == 1
    assert "<background>" not in original_config
    profile = (
        "<background><profile>default</profile>"
        f"<use_reader_executor>{request.param}</use_reader_executor>"
        "<remote_filesystem_read_method>read</remote_filesystem_read_method>"
        "<use_page_cache_for_disks_without_file_cache>0</use_page_cache_for_disks_without_file_cache>"
        "</background>"
    )
    try:
        with node.with_replace_config(
            config_path, original_config.replace("</profiles>", profile + "</profiles>")
        ):
            node.restart_clickhouse()
            yield request.param
    finally:
        broken_s3.reset()
        node.restart_clickhouse()


@pytest.fixture
def s3_cancellation(cluster, broken_s3, s3_cancellation_read_mode):
    node = cluster.instances["node"]
    table = "cancel_s3"
    try:
        yield node, table
    finally:
        broken_s3.reset()
        node.query(f"SYSTEM START MERGES {table}")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def wait_for_s3_request(broken_s3, kind, count=1):
    wait_condition(
        broken_s3.get_request_counts,
        lambda counts: counts[kind] >= count,
        max_attempts=100,
    )


def assert_s3_cancelled(node, table, request):
    node.query(f"SYSTEM STOP MERGES {table}")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.merges WHERE database=currentDatabase() AND table='{table}'",
        "0",
        retry_count=20,
        sleep_time=0.1,
    )
    assert "ABORTED" in request.get_error()


def test_kill_foreground_optimize_stops_s3_retries(s3_cancellation, broken_s3):
    node, table = s3_cancellation
    query_id = "cancel_optimize_" + uuid.uuid4().hex
    node.query(
        f"CREATE TABLE {table} (key UInt32, value String) ENGINE=MergeTree ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "max_bytes_to_merge_at_max_space_in_pool=1, max_bytes_to_merge_at_min_space_in_pool=1"
    )
    # The size limits prevent background selection but do not restrict `OPTIMIZE FINAL`.
    for offset in (0, 1000):
        node.query(
            f"INSERT INTO {table} SELECT number + {offset}, toString(number) FROM numbers(1000)"
        )

    broken_s3.reset()
    broken_s3.setup_at_object_upload(action="internal_error", count=10000)
    try:
        request = node.get_query_request(
            f"OPTIMIZE TABLE {table} FINAL", query_id=query_id, timeout=30
        )
        wait_for_s3_request(broken_s3, "object_upload", count=2)
        node.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC", timeout=20)
        assert "QUERY_WAS_CANCELLED" in request.get_error()
        assert node.query(
            f"SELECT count() FROM system.merges WHERE database=currentDatabase() AND table='{table}'"
        ).strip() == "0"
        assert node.query(f"SELECT count(), sum(key) FROM {table}").strip() == "2000\t1999000"
    finally:
        broken_s3.reset()
        node.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC", timeout=30)


@pytest.mark.parametrize("engine", ["MergeTree", "ReplicatedMergeTree('/cancel_teardown', 'r1')"])
@pytest.mark.parametrize("recover", [True, False], ids=["recovery", "exhaustion"])
def test_cancelled_merge_teardown_retries_multipart_abort(
    s3_cancellation, broken_s3, cluster, engine, recover
):
    node, table = s3_cancellation
    failpoint = "merge_task_pause_after_write"
    optimize_query_id = "cleanup_cancel_" + uuid.uuid4().hex
    prefix = "data/cleanup_retry/"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{cluster.minio_ip}:{cluster.minio_port}",
        aws_access_key_id=cluster.minio_access_key,
        aws_secret_access_key=cluster.minio_secret_key,
        region_name="us-east-1",
    )

    def outstanding_uploads():
        # MinIO treats a nonempty multipart-list prefix as an exact object key.
        # List live uploads and filter locally; do not mistake an empty prefix lookup for successful cleanup.
        return [
            upload
            for page in s3.get_paginator("list_multipart_uploads").paginate(
                Bucket=cluster.minio_bucket
            )
            for upload in page.get("Uploads", [])
            if upload["Key"].startswith(prefix)
        ]

    def object_names():
        return {obj.object_name for obj in list_objects(cluster, prefix)}

    node.query(
        f"CREATE TABLE {table} (key UInt32, value String) ENGINE={engine} ORDER BY key "
        "SETTINGS storage_policy='broken_s3_cleanup_retries', "
        "enable_vertical_merge_algorithm=0, min_bytes_for_full_part_storage=0, "
        "min_rows_for_wide_part=0, min_bytes_for_wide_part=0, "
        "index_granularity_bytes=0, merge_max_block_size_bytes=0"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    for offset in (0, 8192):
        node.query(
            f"INSERT INTO {table} SELECT number + {offset}, randomString(8192) FROM numbers(8192)"
        )
    original_objects = object_names()
    assert not outstanding_uploads()
    try:
        broken_s3.reset()
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node.query(f"SYSTEM START MERGES {table}")
        # Plain `OPTIMIZE` executes synchronously; replicated `OPTIMIZE` schedules the background executor.
        request = node.get_query_request(
            f"OPTIMIZE TABLE {table} FINAL", query_id=optimize_query_id, timeout=30
        )
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        wait_for_s3_request(broken_s3, "part_upload")
        uploads = outstanding_uploads()
        assert uploads, "The paused merge must own real incomplete multipart uploads"
        broken_s3.setup_action(
            "at_abort_multipart_upload", action="internal_error", count=1 if recover else 10000
        )
        node.query(f"SYSTEM STOP MERGES {table}")
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.merges WHERE database=currentDatabase() AND table='{table}'",
            "0",
            retry_count=100,
            sleep_time=0.1,
        )
        if engine == "MergeTree":
            assert "ABORTED" in request.get_error()
        assert broken_s3.get_request_counts()["abort_multipart_upload"] > len(uploads)
        if recover:
            wait_condition(outstanding_uploads, lambda remaining: not remaining, max_attempts=30)
            wait_condition(object_names, lambda remaining: remaining <= original_objects, max_attempts=30)
        else:
            # Exhaustion must terminate teardown, but does not promise eventual garbage collection.
            remaining_ids = {upload["UploadId"] for upload in outstanding_uploads()}
            assert {upload["UploadId"] for upload in uploads} <= remaining_ids
    finally:
        # A replicated `OPTIMIZE` waits for queue completion even after its current merge was cancelled.
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        node.query(f"KILL QUERY WHERE query_id='{optimize_query_id}' SYNC")
        broken_s3.reset()
        for upload in outstanding_uploads():
            s3.abort_multipart_upload(
                Bucket=cluster.minio_bucket, Key=upload["Key"], UploadId=upload["UploadId"]
            )


def test_local_projection_observes_parent_partition_cancellation(s3_cancellation):
    node, table = s3_cancellation
    source = table + "_projection_source"
    projection_pause = "merge_task_projection_stage_pause"
    write_pause = "merge_task_pause_after_write"
    node.query(
        f"CREATE TABLE {table} (partition UInt32, key UInt32, value UInt32, "
        "PROJECTION p (SELECT * ORDER BY value)) ENGINE=MergeTree "
        "PARTITION BY partition ORDER BY key SETTINGS enable_vertical_merge_algorithm=0"
    )
    node.query(f"CREATE TABLE {source} AS {table}")
    node.query(f"SYSTEM STOP MERGES {table}")
    for offset in (0, 10000):
        node.query(f"INSERT INTO {table} SELECT 7, number + {offset}, number FROM numbers(10000)")
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {projection_pause}")
        node.query(f"SYSTEM START MERGES {table}")
        request = node.get_query_request(f"OPTIMIZE TABLE {table} FINAL", timeout=30)
        node.query(f"SYSTEM WAIT FAILPOINT {projection_pause} PAUSE", timeout=30)
        # Base-part writes are done. A cancelled child must never reach its first write.
        node.query(f"SYSTEM ENABLE FAILPOINT {write_pause}")
        replacement_query_id = "projection_cancel_" + uuid.uuid4().hex
        replacement = node.get_query_request(
            f"ALTER TABLE {table} REPLACE PARTITION 7 FROM {source} "
            "SETTINGS allow_replace_partition_from_empty_source=1",
            timeout=10,
            query_id=replacement_query_id,
        )
        node.wait_for_log_line(
            f"{replacement_query_id}.*StorageMergeTree::stopMergesAndWaitForPartition Waiting",
            timeout=10,
        )
        node.query(f"SYSTEM DISABLE FAILPOINT {projection_pause}")
        replacement.get_answer()
        assert "ABORTED" in request.get_error()
        assert node.query(f"SELECT count() FROM {table}").strip() == "0"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {projection_pause}")
        node.query(f"SYSTEM DISABLE FAILPOINT {write_pause}")
        node.query(f"DROP TABLE {source} SYNC")


def test_partition_cancellation_is_isolated(s3_cancellation, broken_s3):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (partition UInt32, key UInt32, value UInt32, unchanged String) "
        "ENGINE=MergeTree PARTITION BY partition ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "always_use_copy_instead_of_hardlinks=1, min_bytes_for_full_part_storage=0, "
        "min_rows_for_wide_part=8192, min_bytes_for_wide_part='10M'"
    )
    node.query(
        f"INSERT INTO {table} SELECT number % 2, number, number, toString(number) FROM numbers(1000)"
    )
    assert node.query(
        f"SELECT partition_id, part_type, part_storage_type FROM system.parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active ORDER BY partition_id"
    ).strip() == "0\tCompact\tFull\n1\tCompact\tFull"
    table_uuid = node.query(
        f"SELECT toString(uuid) FROM system.tables "
        f"WHERE database=currentDatabase() AND name='{table}'"
    ).strip()
    copy_sources = []
    for partition in (0, 1):
        # A logical file can span several objects. Block only one copy per partition.
        remote_paths = (
            node.query(
                "SELECT remote_path FROM system.remote_data_paths "
                "WHERE disk_name='broken_s3_long_retries' "
                f"AND local_path LIKE '%{table_uuid}%' "
                f"AND local_path LIKE '%/{partition}_%/data.bin' "
                "ORDER BY remote_path LIMIT 1"
            )
            .strip()
            .splitlines()
        )
        assert len(remote_paths) == 1, remote_paths
        copy_sources.append(f"root/{remote_paths[0].lstrip('/')}")
    node.query(f"SYSTEM STOP MERGES {table}")
    # Clone untouched parts so both partitions perform independent S3 copies.
    node.query(f"ALTER TABLE {table} UPDATE value = value + 1 WHERE key = 1000 SETTINGS mutations_sync=0")
    source = table + "_empty_source"
    node.query(f"CREATE TABLE {source} AS {table}")
    try:
        broken_s3.reset()
        # Pool admission is not cancellable. Keep only one copy per partition in retry to avoid exhausting the shared pools.
        broken_s3.setup_at_object_copy(
            action="internal_error", count=10000, copy_sources=copy_sources
        )
        node.query(f"SYSTEM START MERGES {table}")
        wait_condition(
            broken_s3.get_copy_sources,
            lambda observed: set(copy_sources).issubset(observed),
            max_attempts=100,
        )
        merges = (
            "SELECT arrayStringConcat(arraySort(groupArray(partition_id)), ',') "
            f"FROM system.merges WHERE database=currentDatabase() AND table='{table}'"
        )
        assert_eq_with_retry(node, merges, "0,1")

        # `REPLACE PARTITION` uses the partition blocker; `DROP PARTITION` stops all merges.
        node.query(
            f"ALTER TABLE {table} REPLACE PARTITION 0 FROM {source} "
            "SETTINGS allow_replace_partition_from_empty_source=1",
            timeout=30,
        )
        assert_eq_with_retry(node, merges, "1")
        broken_s3.reset()
        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.mutations WHERE database=currentDatabase() "
            f"AND table='{table}' AND NOT is_done",
            "0",
        )
        assert node.query(
            f"SELECT count(), countIf(partition != 1 OR value != key) FROM {table}"
        ).strip() == "500\t0"
    except Exception:
        logging.error(
            "Partition cancellation failure before resetting S3:\n%s",
            node.query(
                "SELECT thread_name, query_id, "
                "arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\\n') AS stack "
                "FROM system.stack_trace WHERE query_id LIKE '%::%' FORMAT Vertical",
                settings={"allow_introspection_functions": 1},
                timeout=15,
            ),
        )
        raise
    finally:
        broken_s3.reset()
        node.query(f"DROP TABLE {source} SYNC")


def test_cancelling_horizontal_text_index_merge_stops_s3_retries(
    s3_cancellation, broken_s3
):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, version UInt32, value String, "
        "INDEX idx_value(value) TYPE text(tokenizer='splitByNonAlpha')) "
        "ENGINE=ReplacingMergeTree(version) ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "enable_vertical_merge_algorithm=0, min_bytes_for_full_part_storage=0"
    )
    node.query(f"SYSTEM STOP MERGES {table}")
    # Keep ordinary column output buffered until the rebuilt text index is finalized.
    node.query(f"INSERT INTO {table} VALUES (1, 1, 'old'), (2, 1, 'retained')")
    node.query(f"INSERT INTO {table} VALUES (1, 2, 'new')")

    failpoint = "text_index_pause_before_write_temporary_segment"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node.query(f"SYSTEM START MERGES {table}")
        request = node.get_query_request(f"OPTIMIZE TABLE {table} FINAL", timeout=30)
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        merge_query_id = node.query(
            "SELECT concat(toString(t.uuid), '::', m.result_part_name) "
            "FROM system.merges AS m INNER JOIN system.tables AS t "
            "ON m.database=t.database AND m.table=t.name "
            f"WHERE m.database=currentDatabase() AND m.table='{table}' "
            "AND m.merge_algorithm='Horizontal'"
        ).strip()
        assert merge_query_id
        log_line = node.count_log_lines()

        broken_s3.reset()
        broken_s3.setup_at_object_upload(action="internal_error", count=10000)
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        wait_for_s3_request(broken_s3, "object_upload", count=2)

        write_log = node.exec_in_container(
            ["tail", "-n", f"+{log_line + 1}", "/var/log/clickhouse-server/clickhouse-server.log"]
        )
        merge_log = "\n".join(
            line for line in write_log.splitlines() if "{" + merge_query_id + "}" in line
        )
        assert any(
            "DiskObjectStorageTransaction: write file " in line
            and "/text_index_tmp/" in line
            for line in merge_log.splitlines()
        ), merge_log
        assert broken_s3.get_request_counts()["part_upload"] == 0
        assert_s3_cancelled(node, table, request)
    finally:
        try:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        finally:
            broken_s3.reset()


@pytest.mark.parametrize(
    "storage_policy",
    [
        "broken_s3_long_retries_always_multi_part",
        "encrypted_broken_s3_long_retries_always_multi_part",
        "cached_broken_s3_long_retries_always_multi_part",
    ],
)
def test_cancelling_vertical_multipart_merge_stops_s3_retries(
    s3_cancellation, broken_s3, storage_policy
):
    node, table = s3_cancellation
    cached_policy = storage_policy == "cached_broken_s3_long_retries_always_multi_part"
    if cached_policy:
        assert (
            node.query(
                "SELECT value FROM system.settings "
                "WHERE name = 'enable_filesystem_cache_on_write_operations'"
            ).strip()
            == "1"
        )
    node.query(
        f"CREATE TABLE {table} (key UInt32, value String, extra String) "
        "ENGINE=MergeTree ORDER BY key "
        f"SETTINGS storage_policy='{storage_policy}', "
        "vertical_merge_algorithm_min_rows_to_activate=0, "
        "vertical_merge_algorithm_min_columns_to_activate=0"
    )
    if cached_policy:
        assert (
            node.query(
                "SELECT cache_on_write_operations FROM system.filesystem_cache_settings "
                "WHERE cache_name = 'cached_broken_s3_long_retries_always_multi_part'"
            ).strip()
            == "1"
        )
    node.query(f"SYSTEM STOP MERGES {table}")
    for offset in (0, 10000):
        node.query(f"INSERT INTO {table} SELECT number + {offset}, toString(number), toString(number) FROM numbers(10000)")

    cache_write_bytes_before = None
    if cached_policy:
        cache_write_bytes_before = int(
            node.query(
                "SELECT coalesce(any(value), 0) FROM system.events "
                "WHERE event = 'CachedWriteBufferCacheWriteBytes'"
            )
        )
    broken_s3.reset()
    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_at_part_upload(action="internal_error", count=10000)
    node.query(f"SYSTEM START MERGES {table}")
    request = node.get_query_request(f"OPTIMIZE TABLE {table} FINAL", timeout=30)
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.merges WHERE table='{table}' AND merge_algorithm='Vertical'",
        "1",
    )
    wait_for_s3_request(broken_s3, "part_upload")
    if cached_policy:
        wait_condition(
            lambda: int(
                node.query(
                    "SELECT coalesce(any(value), 0) FROM system.events "
                    "WHERE event = 'CachedWriteBufferCacheWriteBytes'"
                )
            ),
            lambda cache_write_bytes: cache_write_bytes > cache_write_bytes_before,
            max_attempts=100,
        )
    assert_s3_cancelled(node, table, request)
    wait_for_s3_request(broken_s3, "abort_multipart_upload")
    assert broken_s3.get_request_counts()["abort_multipart_upload"] == 1


@pytest.mark.parametrize(
    "part_storage_type,min_bytes_for_full_part_storage",
    [("Full", "0"), ("Packed", "10M")],
)
def test_cancelling_untouched_mutation_copy_stops_s3_retries(
    s3_cancellation, broken_s3, part_storage_type, min_bytes_for_full_part_storage
):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value String) "
        "ENGINE=MergeTree ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "always_use_copy_instead_of_hardlinks=1, "
        f"min_bytes_for_full_part_storage='{min_bytes_for_full_part_storage}'"
    )
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(10000)")
    assert node.query(
        f"SELECT part_storage_type FROM system.parts WHERE database=currentDatabase() AND table='{table}' AND active"
    ).strip() == part_storage_type

    broken_s3.reset()
    broken_s3.setup_at_object_upload(action="internal_error", after=1, count=10000)
    request = node.get_query_request(
        f"ALTER TABLE {table} UPDATE value = value WHERE key < 0 SETTINGS mutations_sync=1",
        timeout=30,
    )
    wait_for_s3_request(broken_s3, "object_upload", count=2)
    assert_s3_cancelled(node, table, request)


@pytest.mark.parametrize(
    "s3_cancellation_read_mode", [0, 1], indirect=True, ids=["legacy", "executor"]
)
def test_cancelling_packed_mutation_copy_source_stops_s3_retries(
    s3_cancellation, broken_s3, s3_cancellation_read_mode
):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value String) "
        "ENGINE=MergeTree ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "always_use_copy_instead_of_hardlinks=1, min_bytes_for_full_part_storage='10M'"
    )
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(10000)")
    assert node.query(
        f"SELECT part_storage_type FROM system.parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active"
    ).strip() == "Packed"

    failpoint = "packed_part_freeze_pause_before_copy"
    try:
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        request = node.get_query_request(
            f"ALTER TABLE {table} UPDATE value = value WHERE key < 0 SETTINGS mutations_sync=1",
            timeout=30,
        )
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=30)
        mutation_query_id = node.query(
            "SELECT concat(toString(t.uuid), '::', m.result_part_name) "
            "FROM system.merges AS m INNER JOIN system.tables AS t "
            "ON m.database=t.database AND m.table=t.name "
            f"WHERE m.database=currentDatabase() AND m.table='{table}'"
        ).strip()
        assert mutation_query_id
        log_line = node.count_log_lines()

        # The predicate finished before this gate. Only the physical copy sees GET failures.
        broken_s3.reset()
        broken_s3.setup_at_object_read(action="internal_error", count=10000)
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        wait_for_s3_request(broken_s3, "object_read", count=2)

        # The packed view wraps a disk pipeline; prove that inner pipeline's actual reader.
        copy_log = node.exec_in_container(
            ["tail", "-n", f"+{log_line + 1}", "/var/log/clickhouse-server/clickhouse-server.log"]
        )
        mutation_log = "\n".join(
            line for line in copy_log.splitlines() if "{" + mutation_query_id + "}" in line
        )
        executor = "ReadPipeline: build: using ReaderExecutor for object storage"
        if s3_cancellation_read_mode:
            assert executor in mutation_log, mutation_log
        else:
            assert "ReadBufferFromRemoteFSGather: Reading from file:" in mutation_log, mutation_log
            assert executor not in mutation_log, mutation_log

        assert_s3_cancelled(node, table, request)
    finally:
        try:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        finally:
            broken_s3.reset()


@pytest.mark.parametrize(
    "storage_policy",
    ["broken_s3_long_retries", "encrypted_broken_s3_long_retries"],
    ids=["plain", "encrypted"],
)
def test_cancelling_partial_mutation_copy_stops_s3_retries(
    s3_cancellation, broken_s3, storage_policy
):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value UInt32, unchanged String) "
        "ENGINE=MergeTree ORDER BY key "
        f"SETTINGS storage_policy='{storage_policy}', "
        "always_use_copy_instead_of_hardlinks=1, min_bytes_for_full_part_storage=0, "
        "min_rows_for_wide_part=0, min_bytes_for_wide_part=0"
    )
    node.query(
        f"INSERT INTO {table} SELECT number, number, toString(number) FROM numbers(10000)"
    )
    assert node.query(
        f"SELECT part_type, part_storage_type, disk_name FROM system.parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active"
    ).strip() == f"Wide\tFull\t{storage_policy}"

    broken_s3.reset()
    broken_s3.setup_at_object_copy(action="internal_error", count=10000)
    request = node.get_query_request(
        f"ALTER TABLE {table} UPDATE value = value + 1 WHERE key = 0 SETTINGS mutations_sync=1",
        timeout=30,
    )
    wait_for_s3_request(broken_s3, "object_copy", count=2)
    assert_s3_cancelled(node, table, request)


def test_cancelling_projection_copy_stops_s3_retries(s3_cancellation, broken_s3):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value UInt32, unchanged String, "
        "PROJECTION p (SELECT unchanged ORDER BY unchanged)) "
        "ENGINE=MergeTree ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', "
        "always_use_copy_instead_of_hardlinks=1, min_bytes_for_full_part_storage=0, "
        "min_rows_for_wide_part=0, min_bytes_for_wide_part=0"
    )
    node.query(
        f"INSERT INTO {table} SELECT number, number, toString(number) FROM numbers(10000)"
    )
    assert node.query(
        f"SELECT part_type, part_storage_type FROM system.parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active"
    ).strip() == "Wide\tFull"
    assert node.query(
        f"SELECT count() FROM system.projection_parts "
        f"WHERE database=currentDatabase() AND table='{table}' AND active AND name='p'"
    ).strip() == "1"

    table_uuid = node.query(
        f"SELECT toString(uuid) FROM system.tables "
        f"WHERE database=currentDatabase() AND name='{table}'"
    ).strip()
    projection_remote_paths = (
        node.query(
            "SELECT remote_path FROM system.remote_data_paths "
            "WHERE disk_name='broken_s3_long_retries' "
            f"AND local_path LIKE '%{table_uuid}%' AND local_path LIKE '%/p.proj/%' "
            "ORDER BY remote_path"
        )
        .strip()
        .splitlines()
    )
    assert projection_remote_paths
    projection_copy_sources = [
        f"root/{remote_path.lstrip('/')}" for remote_path in projection_remote_paths
    ]

    broken_s3.reset()
    broken_s3.setup_at_object_copy(
        action="internal_error", count=10000, copy_sources=projection_copy_sources
    )
    request = node.get_query_request(
        f"ALTER TABLE {table} UPDATE value = value + 1 WHERE key = 0 SETTINGS mutations_sync=1",
        timeout=30,
    )
    wait_condition(
        lambda: (broken_s3.get_request_counts(), broken_s3.get_copy_sources()),
        lambda observed: observed[0]["object_copy_injected"] >= 2
        and bool(set(projection_copy_sources).intersection(observed[1])),
        max_attempts=100,
    )
    assert_s3_cancelled(node, table, request)


def test_cancelling_mutation_copy_source_stops_s3_retries(
    s3_cancellation, broken_s3
):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value String) "
        "ENGINE=MergeTree ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries', always_use_copy_instead_of_hardlinks=1, "
        "min_bytes_for_full_part_storage=0"
    )
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(10000)")
    assert node.query(
        f"SELECT part_storage_type FROM system.parts WHERE database=currentDatabase() AND table='{table}' AND active"
    ).strip() == "Full"

    # Arm read faults before starting the mutation, but only for objects already
    # seen as native copy sources. Ordinary mutation reads must remain unaffected.
    broken_s3.reset()
    broken_s3.setup_at_object_copy(action="access_denied", count=10000)
    broken_s3.setup_at_object_read(action="internal_error", count=10000, after_copy=True)
    request = node.get_query_request(
        f"ALTER TABLE {table} UPDATE value = value WHERE key < 0 SETTINGS mutations_sync=1",
        timeout=30,
    )
    wait_for_s3_request(broken_s3, "object_read_injected", count=2)
    assert_s3_cancelled(node, table, request)


@pytest.mark.parametrize("operation", ["merge", "mutation"])
def test_keeper_expiry_cancels_s3_retries(s3_cancellation, broken_s3, operation):
    node, table = s3_cancellation
    node.query(
        f"CREATE TABLE {table} (key UInt32, value UInt32) "
        f"ENGINE=ReplicatedMergeTree('/test/{table}', 'r1') ORDER BY key "
        "SETTINGS storage_policy='broken_s3_long_retries'"
    )
    if operation == "merge":
        # Only the explicit `OPTIMIZE` should assign this merge.
        node.query(f"ALTER TABLE {table} MODIFY SETTING max_replicated_merges_in_queue=0")
    node.query(f"SYSTEM STOP MERGES {table}")
    for part in range(1 if operation == "mutation" else 2):
        node.query(f"INSERT INTO {table} SELECT number + {part * 1000}, number FROM numbers(1000)")

    broken_s3.reset()
    broken_s3.setup_at_object_upload(action="internal_error", count=10000)
    if operation == "mutation":
        node.query(f"ALTER TABLE {table} UPDATE value = value + 1 WHERE 1")
    node.query(f"SYSTEM START MERGES {table}")
    if operation == "merge":
        node.query(f"OPTIMIZE TABLE {table} FINAL SETTINGS alter_sync=0")
    wait_for_s3_request(broken_s3, "object_upload", count=2)
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.merges WHERE table='{table}' AND is_mutation={int(operation == 'mutation')}",
        "1",
    )

    with PartitionManager() as partition:
        partition.drop_instance_zk_connections(node)
        assert_eq_with_retry(
            node,
            f"SELECT is_session_expired AND is_readonly FROM system.replicas WHERE table='{table}'",
            "1",
            retry_count=60,
            sleep_time=0.5,
        )
        assert_eq_with_retry(
            node,
            f"SELECT count() FROM system.merges WHERE table='{table}'",
            "0",
            retry_count=20,
            sleep_time=0.1,
        )

    # Keeper recovery must not wait for S3 recovery. New work may retry again.
    assert_eq_with_retry(
        node,
        f"SELECT is_readonly OR is_session_expired FROM system.replicas WHERE table='{table}'",
        "0",
    )
    broken_s3.reset()
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.mutations WHERE table='{table}' AND NOT is_done",
        "0",
    )
    node.query(f"OPTIMIZE TABLE {table} FINAL", timeout=30)
    expected = "1000\t500500\n" if operation == "mutation" else "2000\t999000\n"
    assert node.query(f"SELECT count(), sum(value) FROM {table}") == expected
    node.query(f"INSERT INTO {table} VALUES (1000, 1000)")


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

    broken_s3.setup_at_object_upload(count=1, after=1)

    node.query("SYSTEM START MERGES merge_canceled_by_s3_errors_when_move")

    node.query("OPTIMIZE TABLE merge_canceled_by_s3_errors_when_move FINAL")

    node.wait_for_log_line("ExpectedError Message: mock s3 injected unretryable error")

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
    pytest.skip(
        "Disabled, will be fixed after https://github.com/ClickHouse/ClickHouse/issues/51152"
    )

    in_flight = in_flight_memory[0]
    memory = in_flight_memory[1]

    node = cluster.instances[node_name]

    # it's bad idea to test something related to memory with sanitizers
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    node.query("DROP TABLE IF EXISTS s3_test SYNC")
    node.query(
        "CREATE TABLE s3_test"
        " ("
        "   key UInt32 CODEC(NONE), value String CODEC(NONE)"
        " )"
        " ENGINE S3('http://resolver:8085/root/data/test-upload.csv', 'minio', '{minio_secret_key}', 'CSV')",
    )

    broken_s3.setup_fake_multpartuploads()
    slow_responses = 10
    slow_timeout = 15
    broken_s3.setup_slow_answers(
        10 * 1024 * 1024, timeout=slow_timeout, count=slow_responses
    )

    query_id = f"INSERT_INTO_S3_ENGINE_QUERY_ID_{in_flight}"
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(50000000)"
        f" SETTINGS "
        f" max_memory_usage={2*memory}"
        ", max_threads=1, optimize_trivial_insert_select=1"  # ParallelFormattingOutputFormat consumption depends on it
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

    # The more in_flight value is the less time CH waits.
    assert int(wait_inflight) / 1000 / 1000 > slow_responses * slow_timeout / in_flight

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

    uuid = node.query("SELECT uuid FROM system.tables WHERE name='s3_test'").strip()

    node.query("SYSTEM STOP MERGES s3_test")

    broken_s3.setup_fake_multpartuploads()
    broken_s3.setup_slow_answers(10 * 1024 * 1024, timeout=10, count=50)

    query_id = f"INSERT_INTO_S3_DISK_QUERY_ID_{uuid}"
    node.query(
        "INSERT INTO s3_test SELECT number, toString(number) FROM numbers(50000000)"
        f" SETTINGS max_memory_usage={2*memory}"
        ", max_insert_block_size=50000000"
        ", min_insert_block_size_rows=50000000"
        ", min_insert_block_size_bytes=1000000000000"
        ", optimize_trivial_insert_select=1",
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


@pytest.mark.parametrize("node_name", ["node"])
def test_metadata_path_works_correctly(cluster, node_name):
    node = cluster.instances[node_name]
    table = "s3_test_metadata_path"
    create_table(node, table)

    response = node.query(f"SELECT data_paths FROM system.tables WHERE name='{table}'")
    data_paths = ast.literal_eval(response)
    assert len(data_paths) >= 1, list

    # Verifies that trailing slash is added correctly: https://github.com/ClickHouse/ClickHouse/issues/80647
    found = False
    for path in data_paths:
        found = found or "/custom_path/" in path
    assert found, data_paths
    node.query(f"DROP TABLE IF EXISTS {table}")


def test_no_object_storage_read_when_evicting_index_marks(cluster):
    # A part whose skip indices are bundled into `skp_idx.packed` must not make mark-cache
    # eviction ask the object storage what the part holds: eviction runs from the part
    # destructor and only needs key strings. When the blob is gone but its metadata entry
    # survives, that read raises `Code: 499` and the exception escapes into
    # `IMergeTreeDataPart::removeIfNeeded`'s catch, which logs it at `<Error>` level.
    node = cluster.instances["node"]
    table = "s3_index_mark_eviction"

    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    # `columns_and_secondary_indices_sizes_lazy_calculation = 0` is load-bearing: it makes
    # loading the part probe the archive, so the part is detached as broken and destroyed and
    # the eviction path under test runs. Without it the only probe is the one under
    # `#ifndef NDEBUG` in `loadRowsCount`, so on a release or sanitizer build the part loads fine
    # and both assertions below read 0 whether the fix is present or not.
    node.query(
        f"""
        CREATE TABLE {table} (k UInt64, v UInt64, INDEX mm v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY k
        SETTINGS storage_policy = 's3', packed_skip_index_max_bytes = 1048576,
                 index_granularity = 8, min_bytes_for_wide_part = 0,
                 columns_and_secondary_indices_sizes_lazy_calculation = 0
        """
    )
    node.query(f"INSERT INTO {table} SELECT number, number * 7 FROM numbers(64)")

    uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '{table}'"
    ).strip()
    assert uuid

    # Several disks in this module share the same MinIO endpoint, so the same blob is
    # reported once per disk -- select the one this table's policy actually writes to.
    rows = node.query(
        f"""
        SELECT DISTINCT disk_name, remote_path FROM system.remote_data_paths
        WHERE local_path LIKE '%{uuid}%' AND local_path LIKE '%skp_idx.packed%'
        ORDER BY ALL
        """
    ).strip()
    packed_paths = sorted(
        {line.split("\t")[1] for line in rows.splitlines() if line.strip()}
    )
    # An empty selector would make the whole test vacuously green: the index has to be
    # packed for the archive probe to exist at all.
    assert len(packed_paths) == 1, rows

    # Delete the archive OBJECT and keep its metadata entry, so the packed probe's
    # metadata-only `existsFile` re-check still answers "present" and rethrows the 404.
    for path in packed_paths:
        assert cluster.minio_client.stat_object(cluster.minio_bucket, path).size > 0
        cluster.minio_client.remove_object(cluster.minio_bucket, path)
        with pytest.raises(Exception) as exc_info:
            cluster.minio_client.stat_object(cluster.minio_bucket, path)
        assert "code: NoSuchKey" in str(exc_info.value)

    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")
    node.query(f"DETACH TABLE {table}")

    # The archive reader is seeded by the writer, so it must be unseeded before the probe
    # can run -- hence the `DETACH`/`ATTACH`. Ordering is load-bearing: deleting the object
    # after the reattach leaves the probe resolved and reproduces nothing.
    # Anchor the log window here: the failing eviction happens while `ATTACH` loads the
    # (now broken) part and destroys it, i.e. BEFORE the `DROP` below.
    log_anchor = node.count_log_lines()

    node.query(f"ATTACH TABLE {table}")
    node.query(f"DROP TABLE {table} SYNC")

    window = node.exec_in_container(
        [
            "bash",
            "-c",
            f"tail -n +{log_anchor + 1} /var/log/clickhouse-server/clickhouse-server.log"
            " > /tmp/index_mark_eviction_window.log; wc -l < /tmp/index_mark_eviction_window.log",
        ]
    )
    assert int(window.strip()) > 0, window

    def count_in_window(pattern):
        return int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f'grep -a "{pattern}" /tmp/index_mark_eviction_window.log | wc -l',
                ]
            ).strip()
        )

    # Positive control: the part-LOAD path legitimately reads `skp_idx.packed` and is
    # unaffected by eviction, so it reports the missing key in both directions. Without
    # this a fixture that silently failed to delete the blob would pass.
    assert count_in_window("Code: 499") > 0

    # The finding: no `Code: 499` may escape part destruction. Keying on the `removeIfNeeded`
    # frame rather than the error code is required -- a bare count is non-zero either way.
    assert (
        int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    'grep -a "removeIfNeeded" /tmp/index_mark_eviction_window.log'
                    ' | grep -a -c "Code: 499" || true',
                ]
            ).strip()
        )
        == 0
    )
