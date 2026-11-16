import logging
import random
import string
import time

import pytest

from helpers.cluster import ClickHouseCluster

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


def list_object_names(cluster):
    minio = cluster.minio_client
    return {
        obj.object_name
        for obj in minio.list_objects(cluster.minio_bucket, "data/", recursive=True)
    }


def cancel_paused_task(node, query):
    """Run `query` (DROP or DETACH) while the table's background task is paused after handing off
    its part, so the task is flagged for deletion and cancelled when it resumes."""
    node.query("SYSTEM WAIT FAILPOINT merge_mutate_task_pause_after_part_handoff PAUSE")
    assert node.query(f"SELECT count() FROM system.merges WHERE table = '{TABLE_NAME}'") == "1\n"
    node.query("SYSTEM ENABLE FAILPOINT merge_tree_background_task_marked_for_deletion")
    request = node.get_query_request(query)
    node.query("SYSTEM WAIT FAILPOINT merge_tree_background_task_marked_for_deletion PAUSE")
    node.query("SYSTEM DISABLE FAILPOINT merge_mutate_task_pause_after_part_handoff")
    node.query("SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion")
    request.get_answer()


@pytest.mark.parametrize("kind", ["merge", "mutation"])
def test_task_canceled_after_part_handoff(cluster, kind):
    node = cluster.instances["node1"]
    objects_before = list_object_names(cluster)
    node.query(
        f"CREATE TABLE {TABLE_NAME} (id UInt64, data String)"
        f" ENGINE=ReplicatedMergeTree('/clickhouse/tables/{TABLE_NAME}_{random_string(10)}', '1')"
        " ORDER BY id SETTINGS storage_policy='s3'"
    )
    node.query(f"SYSTEM STOP MERGES {TABLE_NAME}")
    for i in range(2 if kind == "merge" else 1):
        node.query(
            f"INSERT INTO {TABLE_NAME} SELECT number, toString(number) FROM numbers({i * 10000}, 10000)"
        )
    try:
        # Only this table's task may reach the pause.
        node.query("SYSTEM STOP MERGES")
        node.query("SYSTEM ENABLE FAILPOINT merge_mutate_task_pause_after_part_handoff")
        node.query(f"SYSTEM START MERGES {TABLE_NAME}")
        if kind == "merge":
            optimize = node.get_query_request(f"OPTIMIZE TABLE {TABLE_NAME} FINAL")
            cancel_paused_task(node, f"DROP TABLE {TABLE_NAME} SYNC")
            optimize.get_answer_and_error()
        else:
            node.query(
                f"ALTER TABLE {TABLE_NAME} UPDATE data = 'x' WHERE 1 SETTINGS mutations_sync = 0"
            )
            cancel_paused_task(node, f"DETACH TABLE {TABLE_NAME}")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT merge_mutate_task_pause_after_part_handoff")
        node.query("SYSTEM DISABLE FAILPOINT merge_tree_background_task_marked_for_deletion")
        node.query("SYSTEM START MERGES")

    if kind == "mutation":
        node.query(f"ATTACH TABLE {TABLE_NAME}")
        assert node.query(f"SELECT count(), sum(id) FROM {TABLE_NAME}") == "10000\t49995000\n"
        node.query_with_retry(
            f"SELECT count() FROM system.mutations WHERE table = '{TABLE_NAME}' AND NOT is_done",
            retry_count=60,
            check_callback=lambda x: x == "0\n",
        )
        node.query(f"DROP TABLE {TABLE_NAME} SYNC")

    for _ in range(30):
        if list_object_names(cluster) == objects_before:
            break
        time.sleep(1)
    assert list_object_names(cluster) == objects_before
