import logging
import time

import pytest
import threading

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/storage_conf.xml",
            ],
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def create_table(node, table_name, additional_settings):
    settings = {
        "storage_policy": "two_disks",
        "old_parts_lifetime": 1,
        "index_granularity": 512,
        "temporary_directories_lifetime": 0,
        "merge_tree_clear_old_temporary_directories_interval_seconds": 1,
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

    node.query(create_table_statement)


@pytest.mark.parametrize("allow_remote_fs_zero_copy_replication", [False, True])
def test_create_table(cluster, allow_remote_fs_zero_copy_replication):
    node = cluster.instances["node1"]

    additional_settings = {}
    table_name = "test_table"

    if allow_remote_fs_zero_copy_replication:
        # different names for logs readability
        table_name = "test_table_zero_copy"
        additional_settings["allow_remote_fs_zero_copy_replication"] = 1

    create_table(node, table_name, additional_settings)

    node.query(
        f"INSERT INTO {table_name} SELECT toDate('2021-01-01') + INTERVAL number % 10 DAY, number, toString(sipHash64(number)) FROM numbers(100_000)"
    )

    stop_alter = False

    def alter():
        d = 0
        node.query(f"ALTER TABLE {table_name} ADD COLUMN col0 String")
        while not stop_alter:
            d = d + 1
            node.query(f"DELETE FROM {table_name} WHERE id < {d}")
            time.sleep(0.1)

    alter_thread = threading.Thread(target=alter)
    alter_thread.start()

    for i in range(1, 10):
        partition = f"2021-01-{i:02d}"
        try:
            node.query(
                f"ALTER TABLE {table_name} MOVE PARTITION '{partition}' TO DISK 's3'",
            )
        except QueryRuntimeException as e:
            if "PART_IS_TEMPORARILY_LOCKED" in str(e):
                continue
            raise e

        # clear old temporary directories wakes up every 1 second
        time.sleep(0.5)

    stop_alter = True
    alter_thread.join()
