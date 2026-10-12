import concurrent.futures
import logging
import random
import string
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

TABLE_NAME = "s3_test"


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "2"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/config.d/storage_conf.xml"],
            macros={"replica": "3"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

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


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, additional_settings=None):
    settings = {
        "storage_policy": "s3",
        # Otherwise the new materialize_statistics_on_insert default writes an extra
        # statistics.packed file per part, breaking the exact S3 object count this test asserts.
        "auto_statistics_types": "",
    }
    settings.update(additional_settings)

    create_table_statement = f"""
        CREATE TABLE {TABLE_NAME} ON CLUSTER cluster(
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=ReplicatedMergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
        """

    list(cluster.instances.values())[0].query(create_table_statement)


def insert(cluster, node_idxs, verify=True):
    all_values = ""
    for node_idx in node_idxs:
        node = cluster.instances["node" + str(node_idx)]
        values = generate_values("2020-01-0" + str(node_idx), 4096)
        node.query(
            f"INSERT INTO {TABLE_NAME} VALUES {values}",
            settings={"insert_quorum": 3},
        )
        if node_idx != 1:
            all_values += ","
        all_values += values

    if verify:
        for node_idx in node_idxs:
            node = cluster.instances["node" + str(node_idx)]
            assert (
                node.query(
                    f"SELECT * FROM {TABLE_NAME} order by dt, id FORMAT Values",
                    settings={"select_sequential_consistency": 1},
                )
                == all_values
            )


def test_custom_query_cancellation_does_not_report_broken_part(cluster):
    create_table(
        cluster,
        additional_settings={
            "min_bytes_for_wide_part": 0,
            "write_marks_for_substreams_in_compact_parts": 1,
        },
    )

    node = cluster.instances["node1"]
    node.query(
        f"INSERT INTO {TABLE_NAME} SELECT toDate('2020-01-01'), number, repeat('x', 1024) FROM numbers(4096)"
    )
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP FILESYSTEM CACHE")

    query_id = uuid.uuid4().hex
    s3_pause_failpoint = "s3_read_before_get_object"
    report_broken_pause_failpoint = "merge_tree_reader_pause_before_report_broken"
    cancellation_failpoint = "query_status_cancel_with_injected_exception"

    node.query(f"SYSTEM ENABLE FAILPOINT {s3_pause_failpoint}")
    node.query(f"SYSTEM ENABLE FAILPOINT {report_broken_pause_failpoint}")
    node.query(f"SYSTEM ENABLE FAILPOINT {cancellation_failpoint}")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    query_future = executor.submit(
        node.query_and_get_answer_with_error,
        f"SELECT sum(id) FROM {TABLE_NAME} SETTINGS "
        "max_threads=1, allow_prefetched_read_pool_for_remote_filesystem=1, "
        "remote_filesystem_read_method='threadpool', remote_filesystem_read_prefetch=1, "
        "filesystem_prefetches_limit=1, enable_filesystem_cache=0, use_uncompressed_cache=0",
        query_id=query_id,
    )

    try:
        node.query(f"SYSTEM WAIT FAILPOINT {s3_pause_failpoint} PAUSE", timeout=60)
        node.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
        assert_eq_with_retry(
            node,
            f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
            "1",
            retry_count=20,
            sleep_time=0.25,
        )
        node.query(f"SYSTEM NOTIFY FAILPOINT {s3_pause_failpoint}")
        node.query(
            f"SYSTEM WAIT FAILPOINT {report_broken_pause_failpoint} PAUSE",
            timeout=60,
        )

        part_checks_before = int(
            node.query(
                "SELECT sum(value) FROM system.events WHERE event='ReplicatedPartChecks'"
            ).strip()
        )
        node.query(f"SYSTEM NOTIFY FAILPOINT {report_broken_pause_failpoint}")

        _, error = query_future.result(timeout=10)
        assert "FAULT_INJECTED" in error, error
        assert "Injected query cancellation exception" in error, error

        node.query("SYSTEM FLUSH LOGS")
        assert (
            node.query(
                "SELECT sum(ProfileEvents['S3GetObject']), "
                "sum(ProfileEvents['ReadBufferFromS3RequestsErrors']) FROM system.query_log "
                f"WHERE query_id='{query_id}' AND type!='QueryStart'"
            ).strip()
            == "0\t0"
        )

        parts_to_check, part_checks_after = map(
            int,
            node.query(
                "SELECT parts_to_check, "
                "(SELECT sum(value) FROM system.events WHERE event='ReplicatedPartChecks') "
                "FROM system.replicas WHERE database=currentDatabase() "
                f"AND table='{TABLE_NAME}'"
            ).strip().split("\t"),
        )
        assert parts_to_check == 0
        assert part_checks_after == part_checks_before
    finally:
        node.query(f"SYSTEM NOTIFY FAILPOINT {s3_pause_failpoint}")
        node.query(f"SYSTEM NOTIFY FAILPOINT {report_broken_pause_failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {s3_pause_failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {report_broken_pause_failpoint}")
        node.query(f"SYSTEM DISABLE FAILPOINT {cancellation_failpoint}")
        executor.shutdown(wait=False, cancel_futures=True)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in list(cluster.instances.values()):
        node.query(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    minio = cluster.minio_client
    # Remove extra objects to prevent tests cascade failing
    for obj in list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True)):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [(0, FILES_OVERHEAD_PER_PART_WIDE), (8192, FILES_OVERHEAD_PER_PART_COMPACT)],
)
def test_insert_select_replicated(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        additional_settings={
            "min_rows_for_wide_part": min_rows_for_wide_part,
            "write_marks_for_substreams_in_compact_parts": 1,
        },
    )

    insert(cluster, node_idxs=[1, 2, 3], verify=True)

    minio = cluster.minio_client
    files = list(minio.list_objects(cluster.minio_bucket, "data/", recursive=True))
    print("List of files:", files)
    assert len(files) == 3 * (FILES_OVERHEAD + files_per_part * 3)


def test_drop_cache_on_cluster(cluster):
    create_table(
        cluster,
        additional_settings={"storage_policy": "s3_cache"},
    )

    insert(cluster, node_idxs=[1, 2, 3], verify=True)

    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    node3 = cluster.instances["node3"]

    node1.query(
        f"select * from clusterAllReplicas(cluster, default, {TABLE_NAME}) format Null"
    )

    assert int(node1.query("select count() from system.filesystem_cache")) > 0
    assert int(node2.query("select count() from system.filesystem_cache")) > 0
    assert int(node3.query("select count() from system.filesystem_cache")) > 0

    node1.query("system drop filesystem cache on cluster cluster")

    assert int(node1.query("select count() from system.filesystem_cache")) == 0
    assert int(node2.query("select count() from system.filesystem_cache")) == 0
    assert int(node3.query("select count() from system.filesystem_cache")) == 0
