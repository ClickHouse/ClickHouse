from random import randint
import pytest
import os.path
import time
import concurrent
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry
import re

cluster = ClickHouseCluster(__file__)

num_nodes = 10


def generate_cluster_def():
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/cluster_for_concurrency_test.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            """
        <clickhouse>
            <remote_servers>
                <cluster>
                    <shard>
        """
        )
        for i in range(num_nodes):
            f.write(
                """
                        <replica>
                            <host>node"""
                + str(i)
                + """</host>
                            <port>9000</port>
                        </replica>
            """
            )
        f.write(
            """
                    </shard>
                </cluster>
            </remote_servers>
        </clickhouse>
        """
        )
    return path


main_configs = ["configs/disallow_concurrency.xml", generate_cluster_def()]
# No [Zoo]Keeper retries for tests with concurrency
user_configs = ["configs/allow_database_types.xml"]

nodes = []
for i in range(num_nodes):
    nodes.append(
        cluster.add_instance(
            f"node{i}",
            main_configs=main_configs,
            user_configs=user_configs,
            external_dirs=["/backups/"],
            macros={"replica": f"node{i}", "shard": "shard1"},
            with_zookeeper=True,
        )
    )

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


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}')"


def create_and_fill_table():
    node0.query(
        "CREATE TABLE tbl ON CLUSTER 'cluster' ("
        "x UInt64"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )
    for i in range(num_nodes):
        nodes[i].query(f"INSERT INTO tbl SELECT number FROM numbers(40000000)")


def wait_for_fail_backup(node, backup_id, backup_name):
    expected_errors = [
        "Concurrent backups not supported",
        f"Backup {backup_name} already exists",
    ]
    status = node.query(
        f"SELECT status FROM system.backups WHERE id == '{backup_id}'"
    ).rstrip("\n")
    # It is possible that the second backup was picked up first, and then the async backup
    if status == "BACKUP_FAILED":
        error = node.query(
            f"SELECT error FROM system.backups WHERE id == '{backup_id}'"
        ).rstrip("\n")
        assert any([expected_error in error for expected_error in expected_errors])
        return
    elif status == "CREATING_BACKUP":
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id = '{backup_id}'",
            "BACKUP_FAILED",
            sleep_time=2,
            retry_count=50,
        )
        error = node.query(
            f"SELECT error FROM system.backups WHERE id == '{backup_id}'"
        ).rstrip("\n")
        assert re.search(f"Backup {backup_name} already exists", error)
        return
    else:
        assert False, "Concurrent backups both passed, when one is expected to fail"


def wait_for_fail_restore(node, restore_id):
    expected_errors = [
        "Concurrent restores not supported",
        "Cannot restore the table default.tbl because it already contains some data",
    ]
    status = node.query(
        f"SELECT status FROM system.backups WHERE id == '{restore_id}'"
    ).rstrip("\n")
    # It is possible that the second backup was picked up first, and then the async backup
    if status == "RESTORE_FAILED":
        error = node.query(
            f"SELECT error FROM system.backups WHERE id == '{restore_id}'"
        ).rstrip("\n")
        assert any([expected_error in error for expected_error in expected_errors])
        return
    elif status == "RESTORING":
        assert_eq_with_retry(
            node,
            f"SELECT status FROM system.backups WHERE id = '{restore_id}'",
            "RESTORE_FAILED",
            sleep_time=2,
            retry_count=50,
        )
        error = node.query(
            f"SELECT error FROM system.backups WHERE id == '{restore_id}'"
        ).rstrip("\n")
        assert re.search(
            "Cannot restore the table default.tbl because it already contains some data",
            error,
        )
        return
    else:
        assert False, "Concurrent restores both passed, when one is expected to fail"


# All the tests have concurrent backup/restores with same backup names
# The same works with different backup names too. Since concurrency
# check comes before backup name check, separate tests are not added for different names


def test_concurrent_backups_on_same_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    id = (
        nodes[0]
        .query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC")
        .split("\t")[0]
    )

    status = (
        nodes[0]
        .query(f"SELECT status FROM system.backups WHERE id == '{id}'")
        .rstrip("\n")
    )
    assert status in ["CREATING_BACKUP", "BACKUP_CREATED"]

    result, error = nodes[0].query_and_get_answer_with_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
    )

    expected_errors = [
        "Concurrent backups not supported",
        f"Backup {backup_name} already exists",
    ]
    if not error:
        wait_for_fail_backup(nodes[0], id, backup_name)

    assert any([expected_error in error for expected_error in expected_errors])

    assert_eq_with_retry(
        nodes[0],
        f"SELECT status FROM system.backups WHERE id = '{id}'",
        "BACKUP_CREATED",
        sleep_time=2,
        retry_count=50,
    )

    # This restore part is added to confirm creating an internal backup & restore work
    # even when a concurrent backup is stopped
    nodes[0].query(
        f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )
    nodes[0].query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")


def test_concurrent_backups_on_different_nodes():
    create_and_fill_table()

    backup_name = new_backup_name()

    id = (
        nodes[1]
        .query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} ASYNC")
        .split("\t")[0]
    )

    status = (
        nodes[1]
        .query(f"SELECT status FROM system.backups WHERE id == '{id}'")
        .rstrip("\n")
    )
    assert status in ["CREATING_BACKUP", "BACKUP_CREATED"]

    result, error = nodes[0].query_and_get_answer_with_error(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}"
    )

    expected_errors = [
        "Concurrent backups not supported",
        f"Backup {backup_name} already exists",
    ]

    if not error:
        wait_for_fail_backup(nodes[1], id, backup_name)

    assert any([expected_error in error for expected_error in expected_errors])

    assert_eq_with_retry(
        nodes[1],
        f"SELECT status FROM system.backups WHERE id = '{id}'",
        "BACKUP_CREATED",
        sleep_time=2,
        retry_count=50,
    )


def test_concurrent_restores_on_same_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    nodes[0].query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    nodes[0].query(
        f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )

    restore_id = (
        nodes[0]
        .query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} ASYNC")
        .split("\t")[0]
    )

    status = (
        nodes[0]
        .query(f"SELECT status FROM system.backups WHERE id == '{restore_id}'")
        .rstrip("\n")
    )
    assert status in ["RESTORING", "RESTORED"]

    result, error = nodes[0].query_and_get_answer_with_error(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}"
    )

    expected_errors = [
        "Concurrent restores not supported",
        "Cannot restore the table default.tbl because it already contains some data",
    ]

    if not error:
        wait_for_fail_restore(nodes[0], restore_id)

    assert any([expected_error in error for expected_error in expected_errors])

    assert_eq_with_retry(
        nodes[0],
        f"SELECT status FROM system.backups WHERE id == '{restore_id}'",
        "RESTORED",
        sleep_time=2,
        retry_count=50,
    )


def test_concurrent_restores_on_different_node():
    create_and_fill_table()

    backup_name = new_backup_name()

    nodes[0].query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name}")

    nodes[0].query(
        f"DROP TABLE tbl ON CLUSTER 'cluster' SYNC",
        settings={
            "distributed_ddl_task_timeout": 360,
        },
    )

    restore_id = (
        nodes[0]
        .query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} ASYNC")
        .split("\t")[0]
    )

    status = (
        nodes[0]
        .query(f"SELECT status FROM system.backups WHERE id == '{restore_id}'")
        .rstrip("\n")
    )
    assert status in ["RESTORING", "RESTORED"]

    result, error = nodes[1].query_and_get_answer_with_error(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}"
    )

    expected_errors = [
        "Concurrent restores not supported",
        "Cannot restore the table default.tbl because it already contains some data",
    ]

    if not error:
        wait_for_fail_restore(nodes[0], restore_id)

    assert any([expected_error in error for expected_error in expected_errors])

    assert_eq_with_retry(
        nodes[0],
        f"SELECT status FROM system.backups WHERE id == '{restore_id}'",
        "RESTORED",
        sleep_time=2,
        retry_count=50,
    )
