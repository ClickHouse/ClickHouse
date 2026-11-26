import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

from .concurrency_helper import (
    add_nodes_to_cluster,
    create_test_table,
    generate_cluster_def,
)

cluster = ClickHouseCluster(__file__)

num_nodes = 2


main_configs = [
    "configs/backups_disk.xml",
    generate_cluster_def(__file__, num_nodes),
]
# No [Zoo]Keeper retries for tests with concurrency
user_configs = ["configs/allow_database_types.xml"]

nodes = add_nodes_to_cluster(cluster, num_nodes, main_configs, user_configs, cpu_limit=10)

node0 = nodes[0]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node0.query(
            "DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC",
            settings={
                "distributed_ddl_task_timeout": 360,
            },
        )


backup_id_counter = 0


def create_and_fill_table() -> None:
    create_test_table(node0)
    for node in nodes:
        node.query("INSERT INTO tbl SELECT number FROM numbers(40000000)")


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def get_status_and_error(node, backup_or_restore_id):
    return (
        node.query(
            f"SELECT status, error FROM system.backups WHERE id == '{backup_or_restore_id}'"
        )
        .rstrip("\n")
        .split("\t")
    )


def wait_for_backup(node, backup_id):
    assert_eq_with_retry(
        node,
        f"SELECT status FROM system.backups WHERE id = '{backup_id}'",
        "BACKUP_CREATED",
        sleep_time=2,
        retry_count=50,
    )


def wait_for_restore(node, restore_id):
    assert_eq_with_retry(
        node,
        f"SELECT status FROM system.backups WHERE id == '{restore_id}'",
        "RESTORED",
        sleep_time=2,
        retry_count=50,
    )


def check_backup_error(error):
    expected_errors = [
        "Concurrent backups are not allowed",
        "BACKUP_ALREADY_EXISTS",
    ]
    assert any([expected_error in error for expected_error in expected_errors])


def check_restore_error(error):
    expected_errors = [
        "Concurrent restores are not allowed",
        "Cannot restore the table default.tbl because it already contains some data",
    ]
    assert any([expected_error in error for expected_error in expected_errors])


def wait_for_backup_failure(node, backup_id):
    status, error = get_status_and_error(node, backup_id)
    # It is possible that the second backup was picked up first, and then the async backup
    if status == "CREATING_BACKUP":
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id = '{backup_id}'",
            "BACKUP_FAILED",
            sleep_time=2,
            retry_count=50,
        )
        status, error = get_status_and_error(node, backup_id)
    if status == "BACKUP_FAILED":
        check_backup_error(error)
    else:
        assert False, "Concurrent backups both passed, when one is expected to fail"


def wait_for_restore_failure(node, restore_id):
    status, error = get_status_and_error(node, restore_id)
    # It is possible that the second backup was picked up first, and then the async backup
    if status == "RESTORING":
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id = '{restore_id}'",
            "RESTORE_FAILED",
            sleep_time=2,
            retry_count=50,
        )
        status, error = get_status_and_error(node, restore_id)
    if status == "RESTORE_FAILED":
        check_restore_error(error)
    else:
        assert False, "Concurrent restores both passed, when one is expected to fail"


# The four tests below reuse one backup name or one restore target, so they also accept the resulting name/target errors.


def test_concurrent_backups_on_same_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    # Backup #1.
    id, status = (
        nodes[0]
        .query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC")
        .rstrip("\n")
        .split("\t")
    )

    assert status in ["CREATING_BACKUP", "BACKUP_CREATED"]

    # Backup #2.
    _, error = nodes[0].query_and_get_answer_with_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
    )

    if error:
        # Backup #2 failed, backup #1 should be successful.
        check_backup_error(error)
        wait_for_backup(nodes[0], id)
    else:
        # Backup #2 was successful, backup #1 should fail.
        wait_for_backup_failure(nodes[0], id)

    # This restore part is added to confirm creating an internal backup & restore work
    # even when a concurrent backup is stopped
    nodes[0].query(
        "DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )
    nodes[0].query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


def test_concurrent_backups_on_different_nodes():
    create_and_fill_table()

    backup_name = new_backup_name()

    # Backup #1.
    id, status = (
        nodes[0]
        .query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC")
        .rstrip("\n")
        .split("\t")
    )

    assert status in ["CREATING_BACKUP", "BACKUP_CREATED"]

    # Backup #2.
    _, error = nodes[1].query_and_get_answer_with_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
    )

    if error:
        # Backup #2 failed, backup #1 should be successful.
        check_backup_error(error)
        wait_for_backup(nodes[0], id)
    else:
        # Backup #2 was successful, backup #1 should fail.
        wait_for_backup_failure(nodes[0], id)

    # This restore part is added to confirm creating an internal backup & restore work
    # even when a concurrent backup is stopped
    nodes[0].query(
        "DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )
    nodes[0].query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


def test_concurrent_restores_on_same_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    nodes[0].query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    nodes[0].query(
        "DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )

    # Restore #1.
    restore_id, status = (
        nodes[0]
        .query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} ASYNC")
        .rstrip("\n")
        .split("\t")
    )

    assert status in ["RESTORING", "RESTORED"]

    # Restore #2.
    _, error = nodes[0].query_and_get_answer_with_error(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}"
    )

    if error:
        # Restore #2 failed, restore #1 should be successful.
        check_restore_error(error)
        wait_for_restore(nodes[0], restore_id)
    else:
        # Restore #2 was successful, restore #1 should fail.
        wait_for_restore_failure(nodes[0], restore_id)


def test_concurrent_restores_on_different_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    nodes[0].query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    nodes[0].query(
        "DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )

    # Restore #1.
    restore_id, status = (
        nodes[0]
        .query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} ASYNC")
        .rstrip("\n")
        .split("\t")
    )

    assert status in ["RESTORING", "RESTORED"]

    # Restore #2.
    _, error = nodes[1].query_and_get_answer_with_error(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}"
    )

    if error:
        # Restore #2 failed, restore #1 should be successful.
        check_restore_error(error)
        wait_for_restore(nodes[0], restore_id)
    else:
        # Restore #2 was successful, restore #1 should fail.
        wait_for_restore_failure(nodes[0], restore_id)


# Distinct destinations and restore targets, so the only possible rejection is the concurrency check.
# The first operation is held inside its coordination by a failpoint.
def test_concurrent_backup_and_restore_rejected_by_default():
    try:
        node0.query("CREATE TABLE tbl_local (x UInt64) ENGINE = MergeTree ORDER BY x")
        node0.query("INSERT INTO tbl_local SELECT number FROM numbers(10)")

        backup_name = new_backup_name()
        try:
            node0.query(
                "SYSTEM ENABLE FAILPOINT backups_pause_before_publishing_progress"
            )
            backup_id = node0.query(
                f"BACKUP TABLE tbl_local TO {backup_name} ASYNC"
            ).split("\t")[0]
            node0.query(
                "SYSTEM WAIT FAILPOINT backups_pause_before_publishing_progress PAUSE",
                timeout=60,
            )
            error = node0.query_and_get_error(
                f"BACKUP TABLE tbl_local TO {new_backup_name()}"
            )
            assert (
                "Concurrent backups are not allowed, turn on the server setting 'backups.allow_concurrent_backups'"
                in error
            )
        finally:
            node0.query(
                "SYSTEM DISABLE FAILPOINT backups_pause_before_publishing_progress"
            )
        wait_for_backup(node0, backup_id)

        try:
            node0.query(
                "SYSTEM ENABLE FAILPOINT restore_pause_before_data_restore_tasks"
            )
            restore_id = node0.query(
                f"RESTORE TABLE tbl_local AS tbl_local_1 FROM {backup_name} ASYNC"
            ).split("\t")[0]
            node0.query(
                "SYSTEM WAIT FAILPOINT restore_pause_before_data_restore_tasks PAUSE",
                timeout=60,
            )
            error = node0.query_and_get_error(
                f"RESTORE TABLE tbl_local AS tbl_local_2 FROM {backup_name}"
            )
            assert (
                "Concurrent restores are not allowed, turn on the server setting 'backups.allow_concurrent_restores'"
                in error
            )
        finally:
            node0.query(
                "SYSTEM DISABLE FAILPOINT restore_pause_before_data_restore_tasks"
            )
        wait_for_restore(node0, restore_id)
    finally:
        node0.query("DROP TABLE IF EXISTS tbl_local SYNC")
        node0.query("DROP TABLE IF EXISTS tbl_local_1 SYNC")
        node0.query("DROP TABLE IF EXISTS tbl_local_2 SYNC")


# Disjoint replicas, so only the cluster-wide check in Keeper can reject the second backup.
def test_concurrent_on_cluster_backups_on_different_replicas_rejected_by_default():
    try:
        for node in nodes:
            node.query(
                "CREATE TABLE tbl_local (x UInt64) ENGINE = MergeTree ORDER BY x"
            )
            node.query("INSERT INTO tbl_local SELECT number FROM numbers(10)")

        try:
            node0.query(
                "SYSTEM ENABLE FAILPOINT backups_pause_before_publishing_progress"
            )
            backup_id = node0.query(
                f"BACKUP TABLE tbl_local ON CLUSTER 'cluster' TO {new_backup_name()} SETTINGS replica_num = 1 ASYNC"
            ).split("\t")[0]
            node0.query(
                "SYSTEM WAIT FAILPOINT backups_pause_before_publishing_progress PAUSE",
                timeout=60,
            )
            error = nodes[1].query_and_get_error(
                f"BACKUP TABLE tbl_local ON CLUSTER 'cluster' TO {new_backup_name()} SETTINGS replica_num = 2"
            )
            assert (
                "Concurrent backups are not allowed, turn on the server setting 'backups.allow_concurrent_backups'"
                in error
            )
        finally:
            node0.query(
                "SYSTEM DISABLE FAILPOINT backups_pause_before_publishing_progress"
            )
        wait_for_backup(node0, backup_id)
    finally:
        for node in nodes:
            node.query("DROP TABLE IF EXISTS tbl_local SYNC")
