import pytest
from kazoo.exceptions import NoNodeError

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

CLUSTER_NAME = "stale_task_name_cluster"
QUEUE_PATH = "/clickhouse/test_ddl_worker_stale_task_name/ddl"
ASYNC_SETTINGS = {
    "distributed_ddl_output_mode": "none",
    "distributed_ddl_task_timeout": 0,
}

cluster = ClickHouseCluster(__file__)

common_configs = ["configs/remote_servers.xml", "configs/distributed_ddl.xml"]
initiator = cluster.add_instance(
    "initiator",
    main_configs=common_configs,
    with_zookeeper=True,
)
worker = cluster.add_instance(
    "worker",
    main_configs=common_configs,
    dictionaries=["configs/dict.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_queue_entries():
    result = initiator.query(
        f"SELECT entry FROM system.distributed_ddl_queue WHERE cluster = '{CLUSTER_NAME}' ORDER BY entry FORMAT TSVRaw"
    )
    return [entry for entry in result.splitlines() if entry]


def wait_for_entries(num_entries):
    initiator.query_with_retry(
        f"SELECT count() FROM (SELECT DISTINCT entry FROM system.distributed_ddl_queue WHERE cluster = '{CLUSTER_NAME}')",
        check_callback=lambda value: value.strip() == str(num_entries),
    )


def wait_for_finished(entry_name, retry_count=60):
    initiator.query_with_retry(
        f"SELECT count() FROM system.distributed_ddl_queue WHERE entry = '{entry_name}' AND status = 'Finished'",
        check_callback=lambda value: value.strip() == "1",
        retry_count=retry_count,
    )


def wait_for_task_queued_in_memory(active_entries, queued_entry, timeout=30):
    """Wait until `queued_entry` is saved in `current_tasks` and blocked behind active workers."""
    worker.wait_for_log_line(
        f"Task {queued_entry} is queued in memory waiting for a free DDL worker slot",
        timeout=timeout,
    )

    active_entries_sql = ", ".join(f"'{entry}'" for entry in active_entries)
    worker.query_with_retry(
        "SELECT "
        f"countIf(entry IN ({active_entries_sql}) AND status = 'Active'), "
        f"countIf(entry = '{queued_entry}' AND status = 'Inactive') "
        "FROM system.distributed_ddl_queue "
        f"WHERE cluster = '{CLUSTER_NAME}'",
        check_callback=lambda value: value.strip() == f"{len(active_entries)}\t1",
    )


def clean_ddl_queue():
    """Remove all entries from the DDL queue in ZooKeeper so each test run starts fresh."""
    zk = cluster.get_kazoo_client("zoo1")
    try:
        for child in zk.get_children(QUEUE_PATH):
            if child.startswith("query-"):
                zk.delete(f"{QUEUE_PATH}/{child}", recursive=True)
    except Exception:
        pass
    finally:
        zk.stop()
        zk.close()


def test_stale_first_failed_task_name_after_deleted_queue_entry(started_cluster):
    smoke_database = "ddl_worker_stale_task_name_smoke"
    worker.query(f"DROP DATABASE IF EXISTS {smoke_database} SYNC")
    clean_ddl_queue()

    for _ in range(4):
        initiator.query(
            f"SYSTEM RELOAD DICTIONARY ON CLUSTER {CLUSTER_NAME} slow_dict_7",
            settings=ASYNC_SETTINGS,
        )

    wait_for_entries(4)
    queue_entries = get_queue_entries()
    deleted_entry = queue_entries[2]
    surviving_entry = queue_entries[3]
    wait_for_task_queued_in_memory(
        active_entries=queue_entries[:2],
        queued_entry=deleted_entry,
    )

    with PartitionManager() as partition_manager:
        partition_manager.drop_instance_zk_connections(
            worker, action="REJECT --reject-with tcp-reset"
        )

        worker.wait_for_log_line("Marking reinitializing")

        zk = cluster.get_kazoo_client("zoo1")
        try:
            zk.delete(f"{QUEUE_PATH}/{deleted_entry}", recursive=True)
        except NoNodeError:
            pass
        finally:
            zk.stop()
            zk.close()

    initiator.query_with_retry(
        f"SELECT count() FROM system.distributed_ddl_queue WHERE entry = '{deleted_entry}'",
        check_callback=lambda value: value.strip() == "0",
        retry_count=60,
    )
    wait_for_finished(surviving_entry)

    initiator.query(
        f"CREATE DATABASE {smoke_database} ON CLUSTER {CLUSTER_NAME}",
        settings={"distributed_ddl_task_timeout": 20},
    )
    worker.query(f"DROP DATABASE IF EXISTS {smoke_database} SYNC")

    assert not worker.contains_in_log("Unexpected error (stale first_failed_task_name")
