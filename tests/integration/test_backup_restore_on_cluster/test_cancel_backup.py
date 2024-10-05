import os
import random
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/cluster.xml",
    "configs/lesser_timeouts.xml",  # Default timeouts are quite big (a few minutes), the tests don't need them to be that big.
    "configs/slow_backups.xml",
    "configs/shutdown_cancel_backups.xml",
]

user_configs = [
    "configs/zookeeper_retries.xml",
]

node1 = cluster.add_instance(
    "node1",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for "test_shutdown_cancel_backup"
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,  # Necessary for "test_shutdown_cancel_backup"
)

nodes = [node1, node2]


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
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' SYNC")


# Utilities


# Gets a printable version the name of a node.
def get_node_name(node):
    return "node1" if (node == node1) else "node2"


# Choose a random instance.
def random_node():
    return random.choice(nodes)


# Makes table "tbl" and fill it with data.
def create_and_fill_table(node, on_cluster=True):
    # We use partitioning so backups would contain more files.
    node.query(
        "CREATE TABLE tbl "
        + ("ON CLUSTER 'cluster' " if on_cluster else "")
        + "(x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') "
        + "ORDER BY tuple() PARTITION BY x%10"
    )
    node.query(f"INSERT INTO tbl SELECT number FROM numbers(100)")


# Generates an ID suitable both as backup id or restore id.
def random_id():
    return uuid.uuid4().hex


# Generates a backup name prepared for using in BACKUP and RESTORE queries.
def get_backup_name(backup_id):
    return f"Disk('backups', '{backup_id}')"


# Reads the status of a backup or a restore from system.backups.
def get_status(initiator, backup_id=None, restore_id=None):
    id = backup_id if backup_id is not None else restore_id
    return initiator.query(f"SELECT status FROM system.backups WHERE id='{id}'").rstrip(
        "\n"
    )


# Reads the error message of a failed backup or a failed restore from system.backups.
def get_error(initiator, backup_id=None, restore_id=None):
    id = backup_id if backup_id is not None else restore_id
    return initiator.query(f"SELECT error FROM system.backups WHERE id='{id}'").rstrip(
        "\n"
    )


# Waits until the status of a backup or a restore becomes a desired one.
# Returns how many seconds the function was waiting.
def wait_status(
    initiator, backup_id=None, restore_id=None, desired="BACKUP_CREATED", timeout=2
):
    print(f"Waiting for status {desired}")
    id = backup_id if backup_id is not None else restore_id
    operation_name = "backup" if backup_id is not None else "restore"
    current_status = get_status(initiator, backup_id=backup_id, restore_id=restore_id)
    waited = 0
    while (current_status != desired) and (timeout > 0):
        sleep_time = min(1, timeout)
        time.sleep(sleep_time)
        timeout -= sleep_time
        waited += sleep_time
        current_status = get_status(
            initiator, backup_id=backup_id, restore_id=restore_id
        )
    duration_str = initiator.query(
        f"SELECT any(end_time - start_time) FROM system.backups WHERE id='{id}'"
    ).rstrip("\n")
    duration = int(duration_str) if duration_str else 0
    print(
        f"{get_node_name(initiator)} : Got status {current_status} for {operation_name} {id} after {waited} seconds (actual duration is {duration})"
    )
    assert current_status == desired
    return duration


# Returns how many entries are in system.processes corresponding to a specified backup or restore.
def get_num_system_processes(
    node=None, backup_id=None, restore_id=None, is_initial_query=None
):
    id = backup_id if backup_id is not None else restore_id
    query_kind = "Backup" if backup_id is not None else "Restore"
    total = 0
    nodes_to_consider = [node] if node is not None else nodes
    filter_for_is_initial_query = (
        f" AND (is_initial_query = {is_initial_query})"
        if is_initial_query is not None
        else ""
    )
    for node in nodes_to_consider:
        count = int(
            node.query(
                f"SELECT count() FROM system.processes WHERE (query_kind='{query_kind}') AND (query LIKE '%{id}%'){filter_for_is_initial_query}"
            )
        )
        total += count
    return total


# Waits until the number of entries in system.processes corresponding to a specified backup or restore becomes a desired one.
# Returns how many seconds the function was waiting.
def wait_num_system_processes(
    node=None,
    backup_id=None,
    restore_id=None,
    is_initial_query=None,
    desired=0,
    timeout=2,
):
    print(f"Waiting for number of system processes = {desired}")
    id = backup_id if backup_id is not None else restore_id
    operation_name = "backup" if backup_id is not None else "restore"
    current_count = get_num_system_processes(
        node=node,
        backup_id=backup_id,
        restore_id=restore_id,
        is_initial_query=is_initial_query,
    )

    def is_current_count_desired():
        return (current_count == desired) or (desired == "1+" and current_count >= 1)

    waited = 0
    while not is_current_count_desired() and (timeout > 0):
        sleep_time = min(1, timeout)
        time.sleep(sleep_time)
        timeout -= sleep_time
        waited += sleep_time
        current_count = get_num_system_processes(
            node=node,
            backup_id=backup_id,
            restore_id=restore_id,
            is_initial_query=is_initial_query,
        )
    if is_current_count_desired():
        print(
            f"Got {current_count} system processes for {operation_name} {id} after {waited} seconds"
        )
    else:
        nodes_to_consider = [node] if node is not None else nodes
        for n in nodes_to_consider:
            count = get_num_system_processes(
                node=n, backup_id=backup_id, restore_id=restore_id
            )
            print(
                f"{get_node_name(n)}: Got {count} system processes for {operation_name} {id} after {waited} seconds"
            )
        assert False
    return waited


# Kills a BACKUP or RESTORE query.
# Returns how many seconds the KILL QUERY was executing.
def kill_query(node, backup_id=None, restore_id=None, is_initial_query=None, timeout=2):
    id = backup_id if backup_id is not None else restore_id
    query_kind = "Backup" if backup_id is not None else "Restore"
    operation_name = "backup" if backup_id is not None else "restore"
    print(f"{get_node_name(node)}: Cancelling {operation_name} {id}")
    filter_for_is_initial_query = (
        f" AND (is_initial_query = {is_initial_query})"
        if is_initial_query is not None
        else ""
    )
    node.query(
        f"KILL QUERY WHERE (query_kind='{query_kind}') AND (query LIKE '%{id}%'){filter_for_is_initial_query} SYNC"
    )
    node.query("SYSTEM FLUSH LOGS")
    duration = (
        int(
            node.query(
                f"SELECT query_duration_ms FROM system.query_log WHERE query_kind='KillQuery' AND query LIKE '%{id}%' AND type='QueryFinish'"
            )
        )
        / 1000
    )
    print(
        f"{get_node_name(node)}: Cancelled {operation_name} {id} after {duration} seconds"
    )
    return duration


# Stops all ZooKeeper servers.
def stop_zookeeper_servers():
    print("Stopping all ZooKeeper servers")
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])


# Starts all ZooKeeper servers back.
def start_zookeeper_servers():
    print("Starting all ZooKeeper servers")
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])


# Disconnects a specified host with ZooKeeper.
def disable_zookeeper_on_host(node):
    print(f"Disconnecting {get_node_name(node)} with ZooKeeper")
    node.rename_config(
        "/etc/clickhouse-server/conf.d/zookeeper_config.xml",
        "zookeeper_config.xml.disabled",
    )
    node.query("SYSTEM RELOAD CONFIG")


# Connects a specified node with ZooKeeper back.
def enable_zookeeper_on_host(node):
    print(f"Connecting {get_node_name(node)} with ZooKeeper back")
    node.rename_config(
        "/etc/clickhouse-server/conf.d/zookeeper_config.xml.disabled",
        "zookeeper_config.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")


# Sleeps for random amount of time.
def random_sleep(max_seconds):
    sleep_time = random.uniform(0, max_seconds)
    print(f"Sleeping {sleep_time} seconds")
    time.sleep(sleep_time)


# Checks that BACKUP and RESTORE cleaned up properly with no trash left in ZooKeeper, backups folder, and logs.
class NoTrashChecker:
    def __init__(self):
        time.sleep(
            1
        )  # Ensure this NoTrashChecker won't collect errors from a possible previous NoTrashChecker.
        self.__start_time_for_collecting_errors = time.gmtime()
        self.__previous_list_of_backups = set(
            os.listdir(os.path.join(node1.cluster.instances_dir, "backups"))
        )

    def check(self, expected_backups=[], expected_errors=[], allowed_errors=[]):
        nodes_in_zookeeper_query_result = node1.query(
            "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/backups' "
            + "AND NOT (name == 'alive_tracker')"
        )
        nodes_in_zookeeper = nodes_in_zookeeper_query_result.splitlines()
        if nodes_in_zookeeper:
            print(f"Found nodes in ZooKeeper: {nodes_in_zookeeper}")
        assert nodes_in_zookeeper == []

        list_of_backups = set(
            os.listdir(os.path.join(node1.cluster.instances_dir, "backups"))
        )
        new_backups = list_of_backups.difference(self.__previous_list_of_backups)
        if new_backups:
            print(f"Found new backups: {new_backups}")
        assert new_backups == set(expected_backups)

        all_errors = set()
        start_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", self.__start_time_for_collecting_errors
        )
        for node in nodes:
            errors_query_result = node.query(
                "SELECT name FROM system.errors WHERE last_error_time >= toDateTime('"
                + start_time
                + "') "
                + "AND NOT ((name == 'KEEPER_EXCEPTION') AND (last_error_message LIKE '%Fault injection%')) "
                + "AND NOT (name == 'NO_ELEMENTS_IN_CONFIG')"
            )
            errors = errors_query_result.splitlines()
            if errors:
                print(f"{get_node_name(node)}: Found errors: {errors}")
                print(
                    node.query(
                        "SELECT name, last_error_message FROM system.errors WHERE last_error_time >= toDateTime('"
                        + start_time
                        + "')"
                    )
                )
            for error in errors:
                assert (error in expected_errors) or (error in allowed_errors)
                all_errors.update(errors)

        not_found_expected_errors = set(expected_errors).difference(all_errors)
        if not_found_expected_errors:
            print(f"Not found expected errors: {not_found_expected_errors}")
            assert False


# Minimum duration for backup and restore operations in this test.
# We test cancelling here so we don't want those too operations to be too fast (otherwise they'll be finished before we cancel them).
min_backup_time = 5
min_restore_time = 5

# Maximum duration for backup and restore operations in this test.
max_backup_time = 120
max_restore_time = 120


# Actual tests


# Test that a BACKUP operation can be cancelled with KILL QUERY.
def test_cancel_backup():
    # Cancel while making a backup.
    no_trash_checker = NoTrashChecker()

    create_and_fill_table(random_node(), on_cluster=True)

    initiator = random_node()
    print(f"Using {get_node_name(initiator)} as initiator")

    backup_id = random_id()
    initiator.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )

    assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
    assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

    # Some delay before we cancel making a backup.
    random_sleep(min_backup_time)

    node_to_cancel, cancel_as_initiator = random.choice(
        [(node1, False), (node2, False), (initiator, True)]
    )
    print(
        f"Cancelling on {'initiator' if cancel_as_initiator else 'node'} {get_node_name(node_to_cancel)}"
    )

    wait_num_system_processes(
        node_to_cancel,
        backup_id=backup_id,
        is_initial_query=cancel_as_initiator,
        desired="1+",
    )
    kill_query(
        node_to_cancel, backup_id=backup_id, is_initial_query=cancel_as_initiator
    )

    wait_status(initiator, backup_id=backup_id, desired="BACKUP_CANCELLED")

    assert "QUERY_WAS_CANCELLED" in get_error(initiator, backup_id=backup_id)
    assert get_num_system_processes(backup_id=backup_id) == 0
    no_trash_checker.check(expected_errors=["QUERY_WAS_CANCELLED"])


# Test that a RESTORE operation can be cancelled with KILL QUERY.
def test_cancel_restore():
    # Make backup.
    print("Will make backup successfully")
    no_trash_checker = NoTrashChecker()
    create_and_fill_table(random_node(), on_cluster=True)
    backup_id = random_id()
    initiator = random_node()
    initiator.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )
    assert (
        wait_status(
            initiator,
            backup_id=backup_id,
            desired="BACKUP_CREATED",
            timeout=max_backup_time,
        )
        > min_backup_time
    )  # Backup shouldn't be too quick for this test.
    assert get_num_system_processes(backup_id=backup_id) == 0
    no_trash_checker.check(expected_backups=[backup_id])

    node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    # Some delay before we cancel restoring.
    print("Will cancel restoring")
    no_trash_checker = NoTrashChecker()

    initiator = random_node()
    print(f"Using {get_node_name(initiator)} as initiator")

    restore_id = random_id()
    initiator.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC"
    )

    assert get_status(initiator, restore_id=restore_id) == "RESTORING"
    assert get_num_system_processes(initiator, restore_id=restore_id) >= 1

    # We shouldn't wait too long, otherwise the restore might be completed before we cancel it.
    random_sleep(min_restore_time)

    node_to_cancel, cancel_as_initiator = random.choice(
        [(node1, False), (node2, False), (initiator, True)]
    )
    print(
        f"Cancelling on {'initiator' if cancel_as_initiator else 'node'} {get_node_name(node_to_cancel)}"
    )

    wait_num_system_processes(
        node_to_cancel,
        restore_id=restore_id,
        is_initial_query=cancel_as_initiator,
        desired="1+",
    )
    kill_query(
        node_to_cancel, restore_id=restore_id, is_initial_query=cancel_as_initiator
    )

    wait_status(initiator, restore_id=restore_id, desired="RESTORE_CANCELLED")

    assert "QUERY_WAS_CANCELLED" in get_error(initiator, restore_id=restore_id)
    assert get_num_system_processes(restore_id=restore_id) == 0
    no_trash_checker.check(expected_errors=["QUERY_WAS_CANCELLED"])

    # Restore successfully.
    print("Will restore from backup successfully")
    no_trash_checker = NoTrashChecker()
    restore_id = random_id()
    initiator = random_node()
    initiator.query(
        f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC"
    )
    assert (
        wait_status(
            initiator,
            restore_id=restore_id,
            desired="RESTORED",
            timeout=max_restore_time,
        )
        > min_restore_time
    )  # Restore shouldn't be too quick for this test.
    assert get_num_system_processes(restore_id=restore_id) == 0
    no_trash_checker.check()


# Test that shutdown cancels a running backup and doesn't wait until it finishes.
def test_shutdown_cancels_backup():
    no_trash_checker = NoTrashChecker()

    create_and_fill_table(random_node(), on_cluster=True)

    initiator = random_node()
    print(f"Using {get_node_name(initiator)} as initiator")

    backup_id = random_id()
    initiator.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )

    assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
    assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

    # Some delay before we shutdown a host which will cancel making a backup.
    random_sleep(min_backup_time)

    node_to_restart = random.choice([node1, node2])
    wait_num_system_processes(node_to_restart, backup_id=backup_id, desired="1+")

    print(f"{get_node_name(node_to_restart)}: Restarting...")
    node_to_restart.restart_clickhouse()  # Must cancel the backup.
    print(f"{get_node_name(node_to_restart)}: Restarted")

    wait_num_system_processes(backup_id=backup_id, desired=0)

    # The information about this cancelled backup must be stored in system.backup_log
    initiator.query("SYSTEM FLUSH LOGS")
    assert initiator.query(
        f"SELECT status FROM system.backup_log WHERE id='{backup_id}' ORDER BY status"
    ) == TSV(["CREATING_BACKUP", "BACKUP_CANCELLED"])

    no_trash_checker.check(expected_errors=["QUERY_WAS_CANCELLED"])


# After an error backup should clean the destination folder and used nodes in ZooKeeper.
# No unexpected errors must be generated.
def test_error_leaves_no_trash():
    no_trash_checker = NoTrashChecker()

    # We create table "tbl" on one node only in order to make "BACKUP TABLE tbl ON CLUSTER" fail
    # (because of the non-existing table on another node).
    create_and_fill_table(random_node(), on_cluster=False)

    initiator = random_node()
    print(f"Using {get_node_name(initiator)} as initiator")

    backup_id = random_id()
    initiator.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )

    wait_status(
        initiator, backup_id=backup_id, desired="BACKUP_FAILED", timeout=max_backup_time
    )
    assert "UNKNOWN_TABLE" in get_error(initiator, backup_id=backup_id)

    assert get_num_system_processes(backup_id=backup_id) == 0
    no_trash_checker.check(expected_errors=["UNKNOWN_TABLE"])


# Long disconnection in the middle of a BACKUP or RESTORE operation shouldn't cause the whole operation's failure.
def test_long_disconnection():
    no_trash_checker = NoTrashChecker()

    create_and_fill_table(random_node())

    initiator = random_node()
    print(f"Using {get_node_name(initiator)} as initiator")

    backup_id = random_id()
    initiator.query(
        f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )

    assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
    assert get_num_system_processes(backup_id=backup_id) >= 1
    wait_num_system_processes(backup_id=backup_id, desired=3)

    # Some delay before we stop some ZooKeepers.
    random_sleep(min_backup_time)

    stop_all_zookeepers, node_to_disconnect = random.choice(
        [(True, None), (False, node1), (False, node2)]
    )

    if stop_all_zookeepers:
        stop_zookeeper_servers()
    else:
        disable_zookeeper_on_host(node_to_disconnect)

    random_sleep(5)

    if stop_all_zookeepers:
        start_zookeeper_servers()
    else:
        enable_zookeeper_on_host(node_to_disconnect)

    wait_status(
        initiator,
        backup_id=backup_id,
        desired="BACKUP_CREATED",
        timeout=max_backup_time,
    )

    assert get_num_system_processes(backup_id=backup_id) == 0

    no_trash_checker.check(
        expected_backups=[backup_id],
        allowed_errors=[
            "KEEPER_EXCEPTION",
            "SOCKET_TIMEOUT",
            "CANNOT_READ_ALL_DATA",
            "NETWORK_ERROR",
            "TABLE_IS_READ_ONLY",
        ],
    )
