import logging
import time

import pytest
import threading
import random

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# two replicas in remote_servers.xml
REPLICA_COUNT = 2


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        for i in range(1, REPLICA_COUNT + 1):
            cluster.add_instance(
                f"node{i}",
                main_configs=[
                    "configs/config.d/storage_conf.xml",
                    "configs/config.d/remote_servers.xml",
                ],
                with_minio=True,
                with_zookeeper=True,
            )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def drop_table(node, table_name, replicated):

    create_table_statement = f"DROP TABLE {table_name} SYNC"

    if replicated:
        node.query_with_retry(create_table_statement)
    else:
        node.query(create_table_statement)


def create_table(node, table_name, replicated, additional_settings):
    settings = {
        "storage_policy": "two_disks",
        "old_parts_lifetime": 0,
        "index_granularity": 512,
        "temporary_directories_lifetime": 0,
        "merge_tree_clear_old_temporary_directories_interval_seconds": 1,
    }
    settings.update(additional_settings)

    table_engine = (
        f"ReplicatedMergeTree('/clickhouse/tables/0/{table_name}', '{node.name}')"
        if replicated
        else "MergeTree()"
    )

    create_table_statement = f"""
        CREATE TABLE {table_name} (
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE = {table_engine}
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}"""

    if replicated:
        node.query_with_retry(create_table_statement)
    else:
        node.query(create_table_statement)


@pytest.mark.parametrize(
    "allow_remote_fs_zero_copy_replication,replicated_engine",
    [(False, False), (False, True), (True, True)],
)
def test_alter_moving(
    cluster, allow_remote_fs_zero_copy_replication, replicated_engine
):
    """
    Test that we correctly move parts during ALTER TABLE
    """

    if replicated_engine:
        nodes = list(cluster.instances.values())
    else:
        nodes = [cluster.instances["node1"]]

    additional_settings = {}

    # Different names for logs readability
    table_name = "test_table"
    if allow_remote_fs_zero_copy_replication:
        table_name = "test_table_zero_copy"
        additional_settings["allow_remote_fs_zero_copy_replication"] = 1
    if replicated_engine:
        table_name = table_name + "_replicated"

    for node in nodes:
        create_table(node, table_name, replicated_engine, additional_settings)

    for i in range(1, 11):
        partition = f"2021-01-{i:02d}"
        random.choice(nodes).query(
            f"INSERT INTO {table_name} SELECT toDate('{partition}'), number as id, toString(sipHash64(number, {i})) FROM numbers(10_000)"
        )

    # Run ALTER in parallel with moving parts

    stop_alter = False

    def alter():
        random.choice(nodes).query(f"ALTER TABLE {table_name} ADD COLUMN col0 String")
        for d in range(1, 100):
            if stop_alter:
                break

            # Some lightweight mutation should change moving part before it is swapped, then we will have to cleanup it.
            # Messages `Failed to swap {}. Active part doesn't exist` should appear in logs.
            #
            # I managed to reproduce issue with DELETE (`ALTER TABLE {table_name} ADD/DROP COLUMN` also works on real s3 instead of minio)
            # Note: do not delete rows with id % 100 = 0, because they are used in `check_count` to use them in check that data is not corrupted
            random.choice(nodes).query(f"DELETE FROM {table_name} WHERE id % 100 = {d}")

            time.sleep(0.1)

    alter_thread = threading.Thread(target=alter)
    alter_thread.start()

    for i in range(1, 11):
        partition = f"2021-01-{i:02d}"
        try:
            random.choice(nodes).query(
                f"ALTER TABLE {table_name} MOVE PARTITION '{partition}' TO DISK 's31'",
            )
        except QueryRuntimeException as e:
            if "PART_IS_TEMPORARILY_LOCKED" in str(e):
                continue
            raise e

        # Function to clear old temporary directories wakes up every 1 second, sleep to make sure it is called
        time.sleep(0.5)

    stop_alter = True
    alter_thread.join()

    # Check that no data was lost

    data_digest = None
    if replicated_engine:
        # We don't know what data was replicated, so we need to check all replicas and take unique values
        data_digest = random.choice(nodes).query_with_retry(
            f"SELECT countDistinct(dt, data) FROM clusterAllReplicas(test_cluster, default.{table_name}) WHERE id % 100 == 0"
        )
    else:
        data_digest = random.choice(nodes).query(
            f"SELECT countDistinct(dt, data) FROM {table_name} WHERE id % 100 == 0"
        )

    assert data_digest == "1000\n"

    for node in nodes:
        drop_table(node, table_name, replicated_engine)


def test_delete_race_leftovers(cluster):
    """
    Test that we correctly delete outdated parts and do not leave any leftovers on s3
    """

    node = cluster.instances["node1"]

    table_name = "test_delete_race_leftovers"
    additional_settings = {
        # use another disk not to interfere with other tests
        "storage_policy": "one_disk",
        # always remove parts in parallel
        "concurrent_part_removal_threshold": 1,
    }

    create_table(
        node, table_name, replicated=True, additional_settings=additional_settings
    )

    # Stop merges to have several small parts in active set
    node.query(f"SYSTEM STOP MERGES {table_name}")

    # Creare several small parts in one partition
    for i in range(1, 11):
        node.query(
            f"INSERT INTO {table_name} SELECT toDate('2021-01-01'), number as id, toString(sipHash64(number, {i})) FROM numbers(10_000)"
        )
    table_digest_query = f"SELECT count(), sum(sipHash64(id, data)) FROM {table_name}"
    table_digest = node.query(table_digest_query)

    # Execute several noop deletes to have parts with updated mutation id without changes in data
    # New parts will have symlinks to old parts
    node.query(f"SYSTEM START MERGES {table_name}")
    for i in range(10):
        node.query(f"DELETE FROM {table_name} WHERE data = ''")

    # Make existing parts outdated
    # Also we don't want have changing parts set,
    # because it will be difficult match objects on s3 and in remote_data_paths to check correctness
    node.query(f"OPTIMIZE TABLE {table_name} FINAL")

    inactive_parts_query = (
        f"SELECT count() FROM system.parts "
        f"WHERE not active AND table = '{table_name}' AND database = 'default'"
    )

    # Try to wait for deletion of outdated parts
    # However, we do not want to wait too long
    # If some parts are not deleted after several iterations, we will just continue
    for i in range(20):
        inactive_parts_count = int(node.query(inactive_parts_query).strip())
        if inactive_parts_count == 0:
            print(f"Inactive parts are deleted after {i} iterations")
            break

        print(f"Inactive parts count: {inactive_parts_count}")
        time.sleep(5)

    # Check that we correctly deleted all outdated parts and no leftovers on s3
    # Do it with retries because we delete blobs in the background
    # and it can be race condition between removing from remote_data_paths and deleting blobs
    all_remote_paths = set()
    known_remote_paths = set()
    for i in range(3):
        known_remote_paths = set(
            node.query(
                f"SELECT remote_path FROM system.remote_data_paths WHERE disk_name = 's32'"
            ).splitlines()
        )

        all_remote_paths = set(
            obj.object_name
            for obj in cluster.minio_client.list_objects(
                cluster.minio_bucket, "data2/", recursive=True
            )
        )

        # Some blobs can be deleted after we listed remote_data_paths
        # It's alright, thus we check only that all remote paths are known
        # (in other words, all remote paths is subset of known paths)
        if all_remote_paths == {p for p in known_remote_paths if p in all_remote_paths}:
            break

        time.sleep(1)

    assert all_remote_paths == {p for p in known_remote_paths if p in all_remote_paths}

    # Check that we have all data
    assert table_digest == node.query(table_digest_query)
    drop_table(node, table_name, replicated=True)
