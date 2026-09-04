import ast
import concurrent.futures
import logging
import os
import signal
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock
from helpers.test_tools import assert_eq_with_retry
from helpers.utility import generate_values, replace_config
from helpers.blobs import wait_blobs_count_synchronization
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


def get_s3_read_histogram_counts(node):
    return {
        metric: int(value)
        for metric, value in (
            line.split("\t")
            for line in node.query(
                "SELECT metric, toUInt64(value) FROM system.histogram_metrics "
                "WHERE metric IN "
                "('s3_read_request_duration_microseconds', 's3_read_request_bytes') "
                "AND labels['le']='+Inf' ORDER BY metric"
            ).strip().splitlines()
        )
    }


@pytest.fixture(scope="function")
def s3_cancellation_table(cluster):
    node = cluster.instances["node"]
    table = f"s3_cancellation_{uuid.uuid4().hex}"
    create_table(node, table, min_bytes_for_wide_part=0)
    node.query(f"ALTER TABLE {table} ADD PROJECTION id_projection INDEX id TYPE basic")
    node.query(
        f"INSERT INTO {table} SELECT toDate('2020-01-01'), number, repeat('x', 1024) FROM numbers(4096)"
    )
    yield node, table
    check_no_objects_after_drop(cluster, table_name=table)


S3_CANCELLATION_SETTINGS = (
    "max_threads=1, load_marks_asynchronously=1, "
    "remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1, "
    "filesystem_prefetches_limit=1, enable_filesystem_cache=0, use_uncompressed_cache=0"
)
REFINER_SETTINGS = (
    "max_rows_to_read=0, max_rows_to_read_leaf=0, use_query_condition_cache=0, "
    "use_skip_indexes=1, use_skip_indexes_on_data_read=1, use_indexes_refiner_in_read_pools=1"
)
PROJECTION_INDEX_SETTINGS = (
    "max_rows_to_read=0, max_rows_to_read_leaf=0, use_query_condition_cache=0, "
    "use_skip_indexes=0, use_skip_indexes_on_data_read=0, use_indexes_refiner_in_read_pools=1, "
    "optimize_use_projections=1, optimize_use_projection_filtering=1, min_table_rows_to_use_projection_index=0"
)


def make_s3_cancellation_query(table, predicate="", extra_settings=""):
    settings = S3_CANCELLATION_SETTINGS
    if extra_settings:
        settings += f", {extra_settings}"
    return f"SELECT sum(id) FROM {table}{predicate} SETTINGS {settings}"


def wait_until_query_is_cancelled(node, query_id):
    assert_eq_with_retry(
        node,
        f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
        "1",
        retry_count=20,
        sleep_time=0.25,
    )


def run_timed_out_query_before_s3(node, query, query_id):
    failpoint = "s3_read_before_get_object"
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    query_future = executor.submit(
        node.query_and_get_answer_with_error, query, query_id=query_id
    )
    try:
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        wait_until_query_is_cancelled(node, query_id)
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        _, error = query_future.result(timeout=10)
        assert "TIMEOUT_EXCEEDED" in error, error
    finally:
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        executor.shutdown(wait=False, cancel_futures=True)


def assert_no_s3_requests(node, query_id):
    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            "SELECT sum(ProfileEvents['S3GetObject']), "
            "sum(ProfileEvents['ReadBufferFromS3RequestsErrors']) FROM system.query_log "
            f"WHERE query_id='{query_id}' AND type!='QueryStart'"
        ).strip()
        == "0\t0"
    )


@pytest.mark.parametrize(
    "predicate,mode_settings",
    [
        ("", "allow_prefetched_read_pool_for_remote_filesystem=1"),
        (
            " WHERE id < 2048",
            f"{REFINER_SETTINGS}, allow_prefetched_read_pool_for_remote_filesystem=1, "
            "filesystem_prefetch_max_memory_usage='1Gi'",
        ),
        (
            " WHERE id < 2048",
            f"{REFINER_SETTINGS}, allow_prefetched_read_pool_for_remote_filesystem=1, "
            "filesystem_prefetch_max_memory_usage=1",
        ),
        (
            " WHERE id < 2048",
            f"{REFINER_SETTINGS}, allow_prefetched_read_pool_for_remote_filesystem=0",
        ),
    ],
    ids=["prefetched", "refiner-prefetched", "refiner-rejected", "refiner-no-pool"],
)
def test_s3_read_stops_after_max_execution_time(
    s3_cancellation_table,
    predicate,
    mode_settings,
):
    node, table = s3_cancellation_table
    query_id = uuid.uuid4().hex
    run_timed_out_query_before_s3(
        node,
        make_s3_cancellation_query(
            table,
            predicate,
            f"{mode_settings}, max_execution_time=1, timeout_overflow_mode='throw'",
        ),
        query_id,
    )
    assert_no_s3_requests(node, query_id)


@pytest.mark.parametrize(
    "cancel_method,expected_exception",
    [
        ("cancel-packet", "QUERY_WAS_CANCELLED_BY_CLIENT"),
        ("disconnect", "ABORTED"),
    ],
)
def test_prefetch_stops_after_native_client_cancel(
    s3_cancellation_table, cancel_method, expected_exception
):
    node, table = s3_cancellation_table
    query_id = uuid.uuid4().hex
    failpoint = "s3_read_before_get_object"

    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    query_request = node.get_query_request(
        make_s3_cancellation_query(
            table, extra_settings="allow_prefetched_read_pool_for_remote_filesystem=1"
        ),
        query_id=query_id,
    )

    try:
        node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        if cancel_method == "disconnect":
            query_request.process.kill()
        else:
            query_request.process.send_signal(signal.SIGINT)

        wait_until_query_is_cancelled(node, query_id)
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")

        answer, error = query_request.get_answer_and_error()
        if cancel_method == "cancel-packet":
            assert answer == "", answer
            assert error == "", error
    finally:
        node.query(f"SYSTEM NOTIFY FAILPOINT {failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        if query_request.process.poll() is None:
            query_request.process.kill()

    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            "SELECT errorCodeToName(exception_code), sum(ProfileEvents['S3GetObject']), "
            "sum(ProfileEvents['ReadBufferFromS3RequestsErrors']) FROM system.query_log "
            f"WHERE query_id='{query_id}' AND type='ExceptionWhileProcessing' "
            "GROUP BY exception_code"
        ).strip()
        == f"{expected_exception}\t0\t0"
    )


@pytest.mark.parametrize(
    "predicate,index_settings",
    [
        (
            "",
            "use_skip_indexes=0, use_skip_indexes_on_data_read=0, optimize_use_projection_filtering=0",
        ),
        (
            " WHERE id < 2048",
            f"{REFINER_SETTINGS}, optimize_use_projection_filtering=0",
        ),
        (
            " WHERE id < 2048",
            PROJECTION_INDEX_SETTINGS,
        ),
    ],
    ids=["prefetched", "skip-index", "projection-index"],
)
def test_prefetch_stops_after_partial_result_cancel(
    s3_cancellation_table, predicate, index_settings
):
    node, table = s3_cancellation_table
    query_id = uuid.uuid4().hex
    s3_failpoint = "s3_read_before_get_object"
    pool_cancel_failpoint = "merge_tree_read_pool_pause_after_cancel"

    node.query(f"SYSTEM ENABLE FAILPOINT {s3_failpoint}")
    node.query(f"SYSTEM ENABLE FAILPOINT {pool_cancel_failpoint}")
    query_request = node.get_query_request(
        make_s3_cancellation_query(
            table,
            predicate,
            extra_settings=f"{index_settings}, allow_prefetched_read_pool_for_remote_filesystem=1, "
            "partial_result_on_first_cancel=1",
        ),
        query_id=query_id,
    )

    try:
        node.query(f"SYSTEM WAIT FAILPOINT {s3_failpoint} PAUSE", timeout=60)
        query_request.process.send_signal(signal.SIGINT)
        node.query(
            f"SYSTEM WAIT FAILPOINT {pool_cancel_failpoint} PAUSE", timeout=60
        )

        assert node.query(
            "SELECT is_cancelled FROM system.processes "
            f"WHERE query_id='{query_id}'"
        ).strip() == "0"
        profile_events_at_cancellation = node.query(
            "SELECT ProfileEvents['S3GetObject'], "
            "ProfileEvents['ReadBufferFromS3RequestsErrors'] FROM system.processes "
            f"WHERE query_id='{query_id}'"
        ).strip()

        node.query(f"SYSTEM NOTIFY FAILPOINT {s3_failpoint}")
        node.query(f"SYSTEM NOTIFY FAILPOINT {pool_cancel_failpoint}")

        answer, error = query_request.get_answer_and_error()
        assert answer.strip() == "0", answer
        assert error == "", error
    finally:
        node.query(f"SYSTEM NOTIFY FAILPOINT {s3_failpoint}")
        node.query(f"SYSTEM NOTIFY FAILPOINT {pool_cancel_failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {s3_failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {pool_cancel_failpoint}")
        if query_request.process.poll() is None:
            query_request.process.kill()

    node.query("SYSTEM FLUSH LOGS")
    assert (
        node.query(
            "SELECT type, ProfileEvents['S3GetObject'], "
            "ProfileEvents['ReadBufferFromS3RequestsErrors'] FROM system.query_log "
            f"WHERE query_id='{query_id}' AND type!='QueryStart'"
        ).strip()
        == f"QueryFinish\t{profile_events_at_cancellation}"
    )


def test_read_big_at_cancellation_does_not_record_s3_histograms(cluster):
    node = cluster.instances["node"]
    object_name = "data/s3_read_big_at_cancellation.parquet"
    url = (
        f"http://{cluster.minio_host}:{cluster.minio_port}/"
        f"{cluster.minio_bucket}/{object_name}"
    )
    table_function = (
        f"s3('{url}', 'minio', '{cluster.minio_secret_key}', "
        "'Parquet', 'id UInt64, data String')"
    )
    query_id = uuid.uuid4().hex

    node.query(
        f"INSERT INTO FUNCTION {table_function} "
        "SELECT number AS id, repeat('x', 1024) AS data FROM numbers(4096)"
    )
    check_process_histograms = not node.with_remote_database_disk
    if check_process_histograms:
        histogram_counts_before = get_s3_read_histogram_counts(node)

    run_timed_out_query_before_s3(
        node,
        f"SELECT sum(id) FROM {table_function} SETTINGS "
        "max_execution_time=1, timeout_overflow_mode='throw', max_threads=1, "
        "max_download_threads=1, remote_filesystem_read_prefetch=0, "
        "enable_filesystem_cache=0, use_uncompressed_cache=0",
        query_id,
    )

    # A remote database disk can perform unrelated S3 reads in background system-log flushes.
    if check_process_histograms:
        assert get_s3_read_histogram_counts(node) == histogram_counts_before

    assert_no_s3_requests(node, query_id)
    cluster.minio_client.remove_object(cluster.minio_bucket, object_name)
    wait_for_delete_s3_objects(cluster, 0, timeout=30)


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

    wait_for_delete_s3_objects(
        cluster, FILES_OVERHEAD_PER_PART_WIDE * 4 - FILES_OVERHEAD_METADATA_VERSION * 4
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

    # Force multi-part upload mode.
    replace_config(
        config_path,
        "<s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>",
        "<s3_max_single_part_upload_size>0</s3_max_single_part_upload_size>",
    )

    node.query("SYSTEM RELOAD CONFIG")

    try:
        s3_requests_before = get_s3_requests()
        node.query(
            "INSERT INTO s3_test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
        )

        # There should be 3 times more S3 requests because multi-part upload mode uses 3 requests to upload object.
        assert get_s3_requests() - s3_requests_before == s3_requests_to_write_partition * 3

        check_no_objects_after_drop(cluster)

    finally:
        # Restore
        replace_config(
            config_path,
            "<s3_max_single_part_upload_size>0</s3_max_single_part_upload_size>",
            "<s3_max_single_part_upload_size>33554432</s3_max_single_part_upload_size>",
        )

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
