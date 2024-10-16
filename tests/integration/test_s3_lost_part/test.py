# Tag no-fasttest: requires S3, no-parallel

import logging
import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.utility import random_string

cluster = ClickHouseCluster(__file__)

logging.getLogger().setLevel(logging.INFO)

replica1 = cluster.add_instance(
    "replica1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "replica1"},
)

replica2 = cluster.add_instance(
    "replica2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    with_minio=True,
    macros={"replica": "replica2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def create_replicated_table(node, table_name):
    engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/shard1/{table_name}', '{{replica}}')"
    )

    node.query_with_retry(
        f"""
        CREATE TABLE {table_name}
        (
            id Int64
        ) 
        ENGINE={engine}
        ORDER BY id
        SETTINGS
            storage_policy='s3',
            allow_remote_fs_zero_copy_replication=1,
            max_cleanup_delay_period=31
        """
    )


def check_replica_before_broke(node, table_name):
    active_parts_name_before_drop = node.query(
        f"SELECT name FROM system.parts WHERE table='{table_name}'"
    ).strip()
    assert active_parts_name_before_drop == "all_0_0_0"

    detached_parts_before_drop = node.query(
        f"SELECT count(*) FROM system.detached_parts WHERE table='{table_name}'"
    ).strip()
    assert detached_parts_before_drop == "0"


def check_replica_after_broke_s3(node, table_name):
    CANCEL_FETCH_MSG_LOG = "Cancel fetch for broken part"

    error = node.query_and_get_error(f"SELECT * FROM {table_name}").strip()
    assert (
        "DB::Exception: The specified key does not exist. This error happened for S3 disk."
        in error
    )

    assert replica1.wait_for_log_line(
        regexp=CANCEL_FETCH_MSG_LOG, timeout=60, look_behind_lines=2000
    )

    detached_parts_name_after_drop = node.query(
        f"SELECT name FROM system.detached_parts WHERE table='{table_name}'"
    ).strip()
    assert detached_parts_name_after_drop == "broken_all_0_0_0"

    REMOVED_PART_MSG_LOG = "Removed 1 old parts"
    assert replica1.wait_for_log_line(
        regexp=REMOVED_PART_MSG_LOG, timeout=60, repetitions=2, look_behind_lines=2000
    )

    data = node.query(f"SELECT * FROM {table_name}").strip()
    assert data == ""


def check_replica_after_insert(node, table_name):
    data = node.query(f"SELECT * FROM {table_name}").strip()
    assert data == "2"

    detached_parts_name_after_drop = node.query(
        f"SELECT name FROM system.detached_parts WHERE table='{table_name}'"
    ).strip()
    assert detached_parts_name_after_drop == "broken_all_0_0_0"


def assert_one_part_exists(node, table_name, expected_part):
    def check_callback(actual_part):
        return actual_part.strip() == expected_part

    part_name = node.query_with_retry(
        f"SELECT name FROM system.parts WHERE table='{table_name}'",
        check_callback=check_callback,
    )
    assert part_name.strip() == expected_part

    assert (
        "1"
        == node.query(
            f"SELECT count(*) FROM system.parts WHERE table='{table_name}'",
        ).strip()
    )


def test_corrupted_blob(start_cluster):
    table_name = "corrupted_blob_" + random_string(8)
    create_replicated_table(replica1, table_name)
    create_replicated_table(replica2, table_name)

    replica1.query(f"INSERT INTO {table_name} VALUES (1)")
    data = replica1.query(f"SELECT * FROM {table_name}").strip()
    assert data == "1"

    uuid = replica1.query(
        f"""
        SELECT uuid
        FROM system.tables
        WHERE name = '{table_name}'
        """
    ).strip()
    assert uuid

    remote_pathes = (
        replica1.query(
            f"""
        SELECT remote_path
        FROM system.remote_data_paths
        WHERE
            local_path LIKE '%{uuid}%'
            AND local_path LIKE '%.bin%'
        ORDER BY ALL
        """
        )
        .strip()
        .split()
    )
    assert len(remote_pathes) > 0

    check_replica_before_broke(replica1, table_name)
    check_replica_before_broke(replica2, table_name)

    for path in remote_pathes:
        assert cluster.minio_client.stat_object(cluster.minio_bucket, path).size > 0
        cluster.minio_client.remove_object(cluster.minio_bucket, path)

    check_replica_after_broke_s3(replica1, table_name)
    check_replica_after_broke_s3(replica2, table_name)

    replica1.query(f"INSERT INTO {table_name} VALUES (2)")

    check_replica_after_insert(replica1, table_name)
    check_replica_after_insert(replica2, table_name)


def test_no_metadata_file(start_cluster):
    table_name = "no_metadata_file_" + random_string(8)
    create_replicated_table(replica1, table_name)
    create_replicated_table(replica2, table_name)

    replica1.query(f"INSERT INTO {table_name} VALUES (1)")
    data = replica1.query(f"SELECT * FROM {table_name}").strip()
    assert "1" == data

    part_name = replica1.query(
        f"SELECT name FROM system.parts WHERE table='{table_name}'"
    ).strip()

    assert "all_0_0_0" == part_name

    table_metadata_path = replica1.query(
        f"SELECT path FROM system.parts WHERE table='{table_name}'"
    ).strip()

    original_path = table_metadata_path + "data.bin"
    new_path = table_metadata_path + "old_data.bin"

    replica1.exec_in_container(["bash", "-c", f"mv {original_path} {new_path}"])

    error = replica1.query_and_get_error(f"SELECT * FROM {table_name}").strip()
    assert "DB::Exception: Cannot open file" in error

    FETCHED_PART_MSG = f"Fetched part {part_name} from"

    assert replica1.wait_for_log_line(
        regexp=FETCHED_PART_MSG, timeout=60, look_behind_lines=2000
    )

    assert_one_part_exists(replica1, table_name, part_name)

    data = replica1.query(f"SELECT * FROM {table_name}").strip()
    assert "1" == data


def test_broken_metadata_file(start_cluster):
    table_name = "broken_metadata_file_" + random_string(8)
    create_replicated_table(replica1, table_name)
    create_replicated_table(replica2, table_name)

    replica1.query(f"INSERT INTO {table_name} VALUES (1)")
    data = replica1.query(f"SELECT * FROM {table_name}").strip()
    assert "1" == data

    part_name = replica1.query(
        f"SELECT name FROM system.parts WHERE table='{table_name}'"
    ).strip()

    assert "all_0_0_0" == part_name

    table_metadata_path = replica1.query(
        f"SELECT path FROM system.parts WHERE table='{table_name}'"
    ).strip()

    original_path = table_metadata_path + "data.bin"

    replica1.exec_in_container(["bash", "-c", f"echo 3 > {original_path}"])

    error = replica1.query_and_get_error(f"SELECT * FROM {table_name}").strip()
    assert "DB::Exception: Attempt to read after eof" in error

    replica1.query(f"DETACH TABLE {table_name} SYNC")
    replica1.query(f"ATTACH TABLE {table_name}")

    FETCHED_PART_MSG = f"Fetched part {part_name} from"

    assert replica1.wait_for_log_line(
        regexp=FETCHED_PART_MSG, timeout=60, look_behind_lines=2000
    )

    assert_one_part_exists(replica1, table_name, part_name)

    data = replica1.query(f"SELECT * FROM {table_name}").strip()
    assert "1" == data
