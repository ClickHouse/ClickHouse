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


def create_table(node, table_name, replicated, additional_settings):
    settings = {
        "storage_policy": "two_disks",
        "old_parts_lifetime": 1,
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
def test_create_table(
    cluster, allow_remote_fs_zero_copy_replication, replicated_engine
):
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
                f"ALTER TABLE {table_name} MOVE PARTITION '{partition}' TO DISK 's3'",
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
