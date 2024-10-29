import os
import random
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_manager import ConfigManager
from helpers.network import PartitionManager
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
def cleanup_after_test():
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
def create_and_fill_table(node, num_parts=10, on_cluster=True):
    # We use partitioning to make sure there will be more files in a backup.
    partition_by_clause = " PARTITION BY x%" + str(num_parts) if num_parts > 1 else ""
    node.query(
        "CREATE TABLE tbl "
        + ("ON CLUSTER 'cluster' " if on_cluster else "")
        + "(x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}') "
        + "ORDER BY tuple()"
        + partition_by_clause
    )
    if num_parts > 0:
        node.query(f"INSERT INTO tbl SELECT number FROM numbers({num_parts})")


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
    initiator,
    status="BACKUP_CREATED",
    backup_id=None,
    restore_id=None,
    timeout=None,
):
    print(f"Waiting for status {status}")
    id = backup_id if backup_id is not None else restore_id
    operation_name = "backup" if backup_id is not None else "restore"
    current_status = get_status(initiator, backup_id=backup_id, restore_id=restore_id)
    waited = 0
    while (
        (current_status != status)
        and (current_status in ["CREATING_BACKUP", "RESTORING"])
        and ((timeout is None) or (waited < timeout))
    ):
        sleep_time = 1 if (timeout is None) else min(1, timeout - waited)
        time.sleep(sleep_time)
        waited += sleep_time
        current_status = get_status(
            initiator, backup_id=backup_id, restore_id=restore_id
        )
    start_time, end_time = (
        initiator.query(
            f"SELECT start_time, end_time FROM system.backups WHERE id='{id}'"
        )
        .splitlines()[0]
        .split("\t")
    )
    print(
        f"{get_node_name(initiator)} : Got status {current_status} for {operation_name} {id} after waiting {waited} seconds "
        f"(start_time = {start_time}, end_time = {end_time})"
    )
    assert current_status == status


# Returns how many entries are in system.processes corresponding to a specified backup or restore.
def get_num_system_processes(
    node_or_nodes, backup_id=None, restore_id=None, is_initial_query=None
):
    id = backup_id if backup_id is not None else restore_id
    query_kind = "Backup" if backup_id is not None else "Restore"
    total = 0
    filter_for_is_initial_query = (
        f" AND (is_initial_query = {is_initial_query})"
        if is_initial_query is not None
        else ""
    )
    nodes_to_consider = (
        node_or_nodes if (type(node_or_nodes) is list) else [node_or_nodes]
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
    node_or_nodes,
    num_system_processes=0,
    backup_id=None,
    restore_id=None,
    is_initial_query=None,
    timeout=None,
):
    print(f"Waiting for number of system processes = {num_system_processes}")
    id = backup_id if backup_id is not None else restore_id
    operation_name = "backup" if backup_id is not None else "restore"
    current_count = get_num_system_processes(
        node_or_nodes,
        backup_id=backup_id,
        restore_id=restore_id,
        is_initial_query=is_initial_query,
    )

    def is_current_count_ok():
        return (current_count == num_system_processes) or (
            num_system_processes == "1+" and current_count >= 1
        )

    waited = 0
    while not is_current_count_ok() and ((timeout is None) or (waited < timeout)):
        sleep_time = 1 if (timeout is None) else min(1, timeout - waited)
        time.sleep(sleep_time)
        waited += sleep_time
        current_count = get_num_system_processes(
            node_or_nodes,
            backup_id=backup_id,
            restore_id=restore_id,
            is_initial_query=is_initial_query,
        )
    if is_current_count_ok():
        print(
            f"Got {current_count} system processes for {operation_name} {id} after waiting {waited} seconds"
        )
    else:
        nodes_to_consider = (
            node_or_nodes if (type(node_or_nodes) is list) else [node_or_nodes]
        )
        for node in nodes_to_consider:
            count = get_num_system_processes(
                node, backup_id=backup_id, restore_id=restore_id
            )
            print(
                f"{get_node_name(node)}: Got {count} system processes for {operation_name} {id} after waiting {waited} seconds"
            )
        assert False
    return waited


# Kills a BACKUP or RESTORE query.
# Returns how many seconds the KILL QUERY was executing.
def kill_query(
    node, backup_id=None, restore_id=None, is_initial_query=None, timeout=None
):
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
    if timeout is not None:
        assert duration < timeout


# Stops all ZooKeeper servers.
def stop_zookeeper_servers(zoo_nodes):
    print(f"Stopping ZooKeeper servers {zoo_nodes}")
    old_time = time.monotonic()
    cluster.stop_zookeeper_nodes(zoo_nodes)
    print(
        f"Stopped ZooKeeper servers {zoo_nodes} in {time.monotonic() - old_time} seconds"
    )


# Starts all ZooKeeper servers back.
def start_zookeeper_servers(zoo_nodes):
    print(f"Starting ZooKeeper servers {zoo_nodes}")
    old_time = time.monotonic()
    cluster.start_zookeeper_nodes(zoo_nodes)
    print(
        f"Started ZooKeeper servers {zoo_nodes} in {time.monotonic() - old_time} seconds"
    )


# Sleeps for random amount of time.
def random_sleep(max_seconds):
    if random.randint(0, 5) > 0:
        sleep(random.uniform(0, max_seconds))


def sleep(seconds):
    print(f"Sleeping {seconds} seconds")
    time.sleep(seconds)


# Checks that BACKUP and RESTORE cleaned up properly with no trash left in ZooKeeper, backups folder, and logs.
class NoTrashChecker:
    def __init__(self):
        self.expect_backups = []
        self.expect_unfinished_backups = []
        self.expect_errors = []
        self.allow_errors = []
        self.check_zookeeper = True

        # Sleep 1 second to ensure this NoTrashChecker won't collect errors from a possible previous NoTrashChecker.
        time.sleep(1)

        self.__start_time_for_collecting_errors = time.gmtime()
        self.__previous_list_of_backups = set(
            os.listdir(os.path.join(node1.cluster.instances_dir, "backups"))
        )

        self.__previous_list_of_znodes = set(
            node1.query(
                "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/backups' "
                + "AND NOT (name == 'alive_tracker')"
            ).splitlines()
        )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        list_of_znodes = set(
            node1.query(
                "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/backups' "
                + "AND NOT (name == 'alive_tracker')"
            ).splitlines()
        )
        new_znodes = list_of_znodes.difference(self.__previous_list_of_znodes)
        if new_znodes:
            print(f"Found nodes in ZooKeeper: {new_znodes}")
            for node in new_znodes:
                print(
                    f"Nodes in '/clickhouse/backups/{node}':\n"
                    + node1.query(
                        f"SELECT name FROM system.zookeeper WHERE path = '/clickhouse/backups/{node}'"
                    )
                )
                print(
                    f"Nodes in '/clickhouse/backups/{node}/stage':\n"
                    + node1.query(
                        f"SELECT name FROM system.zookeeper WHERE path = '/clickhouse/backups/{node}/stage'"
                    )
                )
        if self.check_zookeeper:
            assert new_znodes == set()

        list_of_backups = set(
            os.listdir(os.path.join(node1.cluster.instances_dir, "backups"))
        )
        new_backups = list_of_backups.difference(self.__previous_list_of_backups)
        unfinished_backups = set(
            backup
            for backup in new_backups
            if not os.path.exists(
                os.path.join(node1.cluster.instances_dir, "backups", backup, ".backup")
            )
        )
        new_backups = set(
            backup for backup in new_backups if backup not in unfinished_backups
        )
        if new_backups:
            print(f"Found new backups: {new_backups}")
        if unfinished_backups:
            print(f"Found unfinished backups: {unfinished_backups}")
        assert new_backups == set(self.expect_backups)
        assert unfinished_backups == set(self.expect_unfinished_backups)

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
                assert (error in self.expect_errors) or (error in self.allow_errors)
                all_errors.update(errors)

        not_found_expected_errors = set(self.expect_errors).difference(all_errors)
        if not_found_expected_errors:
            print(f"Not found expected errors: {not_found_expected_errors}")
            assert False


__backup_id_of_successful_backup = None


# Generates a backup which will be used to test RESTORE.
def get_backup_id_of_successful_backup():
    global __backup_id_of_successful_backup
    if __backup_id_of_successful_backup is None:
        __backup_id_of_successful_backup = random_id()
        with NoTrashChecker() as no_trash_checker:
            print("Will make backup successfully")
            backup_id = __backup_id_of_successful_backup
            create_and_fill_table(random_node())
            initiator = random_node()
            print(f"Using {get_node_name(initiator)} as initiator")
            initiator.query(
                f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
            )
            wait_status(initiator, "BACKUP_CREATED", backup_id=backup_id)
            assert get_num_system_processes(nodes, backup_id=backup_id) == 0
            no_trash_checker.expect_backups = [backup_id]

            # Dropping the table before restoring.
            node1.query("DROP TABLE tbl ON CLUSTER 'cluster' SYNC")

    return __backup_id_of_successful_backup


# Actual tests


# Test that a BACKUP operation can be cancelled with KILL QUERY.
def test_cancel_backup():
    with NoTrashChecker() as no_trash_checker:
        create_and_fill_table(random_node())

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        backup_id = random_id()
        initiator.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
        )

        assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
        assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

        # We shouldn't wait too long here, because otherwise the backup might be completed before we cancel it.
        random_sleep(3)

        node_to_cancel, cancel_as_initiator = random.choice(
            [(node1, False), (node2, False), (initiator, True)]
        )

        wait_num_system_processes(
            node_to_cancel,
            "1+",
            backup_id=backup_id,
            is_initial_query=cancel_as_initiator,
        )

        print(
            f"Cancelling on {'initiator' if cancel_as_initiator else 'node'} {get_node_name(node_to_cancel)}"
        )

        # The timeout is 2 seconds here because a backup must be cancelled quickly.
        kill_query(
            node_to_cancel,
            backup_id=backup_id,
            is_initial_query=cancel_as_initiator,
            timeout=3,
        )

        if cancel_as_initiator:
            assert get_status(initiator, backup_id=backup_id) == "BACKUP_CANCELLED"
        wait_status(initiator, "BACKUP_CANCELLED", backup_id=backup_id, timeout=3)

        assert "QUERY_WAS_CANCELLED" in get_error(initiator, backup_id=backup_id)
        assert get_num_system_processes(nodes, backup_id=backup_id) == 0
        no_trash_checker.expect_errors = ["QUERY_WAS_CANCELLED"]


# Test that a RESTORE operation can be cancelled with KILL QUERY.
def test_cancel_restore():
    # Make backup.
    backup_id = get_backup_id_of_successful_backup()

    # Cancel restoring.
    with NoTrashChecker() as no_trash_checker:
        print("Will cancel restoring")
        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        restore_id = random_id()
        initiator.query(
            f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC"
        )

        assert get_status(initiator, restore_id=restore_id) == "RESTORING"
        assert get_num_system_processes(initiator, restore_id=restore_id) >= 1

        # We shouldn't wait too long here, because otherwise the restore might be completed before we cancel it.
        random_sleep(3)

        node_to_cancel, cancel_as_initiator = random.choice(
            [(node1, False), (node2, False), (initiator, True)]
        )

        wait_num_system_processes(
            node_to_cancel,
            "1+",
            restore_id=restore_id,
            is_initial_query=cancel_as_initiator,
        )

        print(
            f"Cancelling on {'initiator' if cancel_as_initiator else 'node'} {get_node_name(node_to_cancel)}"
        )

        # The timeout is 2 seconds here because a restore must be cancelled quickly.
        kill_query(
            node_to_cancel,
            restore_id=restore_id,
            is_initial_query=cancel_as_initiator,
            timeout=3,
        )

        if cancel_as_initiator:
            assert get_status(initiator, restore_id=restore_id) == "RESTORE_CANCELLED"
        wait_status(initiator, "RESTORE_CANCELLED", restore_id=restore_id, timeout=3)

        assert "QUERY_WAS_CANCELLED" in get_error(initiator, restore_id=restore_id)
        assert get_num_system_processes(nodes, restore_id=restore_id) == 0
        no_trash_checker.expect_errors = ["QUERY_WAS_CANCELLED"]

    # Restore successfully.
    with NoTrashChecker() as no_trash_checker:
        print("Will restore from backup successfully")
        restore_id = random_id()
        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        initiator.query(
            f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC"
        )

        wait_status(initiator, "RESTORED", restore_id=restore_id)
        assert get_num_system_processes(nodes, restore_id=restore_id) == 0


# Test that shutdown cancels a running backup and doesn't wait until it finishes.
def test_shutdown_cancels_backup():
    with NoTrashChecker() as no_trash_checker:
        create_and_fill_table(random_node())

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        backup_id = random_id()
        initiator.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
        )

        assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
        assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

        # We shouldn't wait too long here, because otherwise the backup might be completed before we cancel it.
        random_sleep(3)

        node_to_restart = random.choice([node1, node2])
        wait_num_system_processes(node_to_restart, "1+", backup_id=backup_id)

        print(f"{get_node_name(node_to_restart)}: Restarting...")
        node_to_restart.restart_clickhouse()  # Must cancel the backup.
        print(f"{get_node_name(node_to_restart)}: Restarted")

        wait_num_system_processes(nodes, 0, backup_id=backup_id)

        if initiator != node_to_restart:
            assert get_status(initiator, backup_id=backup_id) == "BACKUP_CANCELLED"
            assert "QUERY_WAS_CANCELLED" in get_error(initiator, backup_id=backup_id)

        # The information about this cancelled backup must be stored in system.backup_log
        initiator.query("SYSTEM FLUSH LOGS")
        assert initiator.query(
            f"SELECT status FROM system.backup_log WHERE id='{backup_id}' ORDER BY status"
        ) == TSV(["CREATING_BACKUP", "BACKUP_CANCELLED"])

        no_trash_checker.expect_errors = ["QUERY_WAS_CANCELLED"]


# After an error backup should clean the destination folder and used nodes in ZooKeeper.
# No unexpected errors must be generated.
def test_error_leaves_no_trash():
    with NoTrashChecker() as no_trash_checker:
        # We create table "tbl" on one node only in order to make "BACKUP TABLE tbl ON CLUSTER" fail
        # (because of the non-existing table on another node).
        create_and_fill_table(random_node(), on_cluster=False)

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        backup_id = random_id()
        initiator.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
        )

        wait_status(initiator, "BACKUP_FAILED", backup_id=backup_id)
        assert "UNKNOWN_TABLE" in get_error(initiator, backup_id=backup_id)

        assert get_num_system_processes(nodes, backup_id=backup_id) == 0
        no_trash_checker.expect_errors = ["UNKNOWN_TABLE"]


# A backup must be stopped if Zookeeper is disconnected longer than `failure_after_host_disconnected_for_seconds`.
def test_long_disconnection_stops_backup():
    with NoTrashChecker() as no_trash_checker, ConfigManager() as config_manager:
        # Config "faster_zk_disconnect_detect.xml" is used in this test to decrease number of retries when reconnecting to ZooKeeper.
        # Without this config this test can take several minutes (instead of seconds) to run.
        config_manager.add_main_config(nodes, "configs/faster_zk_disconnect_detect.xml")

        create_and_fill_table(random_node(), num_parts=100)

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        backup_id = random_id()
        initiator.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC",
            settings={"backup_restore_failure_after_host_disconnected_for_seconds": 3},
        )

        assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
        assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

        no_trash_checker.expect_unfinished_backups = [backup_id]
        no_trash_checker.allow_errors = [
            "FAILED_TO_SYNC_BACKUP_OR_RESTORE",
            "KEEPER_EXCEPTION",
            "SOCKET_TIMEOUT",
            "CANNOT_READ_ALL_DATA",
            "NETWORK_ERROR",
            "TABLE_IS_READ_ONLY",
        ]
        no_trash_checker.check_zookeeper = False

        with PartitionManager() as pm:
            random_sleep(3)

            time_before_disconnection = time.monotonic()

            node_to_drop_zk_connection = random_node()
            print(
                f"Dropping connection between {get_node_name(node_to_drop_zk_connection)} and ZooKeeper"
            )
            pm.drop_instance_zk_connections(node_to_drop_zk_connection)

            # Being disconnected from ZooKeeper a backup is expected to fail.
            wait_status(initiator, "BACKUP_FAILED", backup_id=backup_id)

            time_to_fail = time.monotonic() - time_before_disconnection
            error = get_error(initiator, backup_id=backup_id)
            print(f"error={error}")
            assert "Lost connection" in error

            # A backup is expected to fail, but it isn't expected to fail too soon.
            print(f"Backup failed after {time_to_fail} seconds disconnection")
            assert time_to_fail > 3
            assert time_to_fail < 30


# A backup must NOT be stopped if Zookeeper is disconnected shorter than `failure_after_host_disconnected_for_seconds`.
def test_short_disconnection_doesnt_stop_backup():
    with NoTrashChecker() as no_trash_checker, ConfigManager() as config_manager:
        use_faster_zk_disconnect_detect = random.choice([True, False])
        if use_faster_zk_disconnect_detect:
            print("Using faster_zk_disconnect_detect.xml")
            config_manager.add_main_config(
                nodes, "configs/faster_zk_disconnect_detect.xml"
            )

        create_and_fill_table(random_node())

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        backup_id = random_id()
        initiator.query(
            f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC",
            settings={"backup_restore_failure_after_host_disconnected_for_seconds": 6},
        )

        assert get_status(initiator, backup_id=backup_id) == "CREATING_BACKUP"
        assert get_num_system_processes(initiator, backup_id=backup_id) >= 1

        # Dropping connection for less than `failure_after_host_disconnected_for_seconds`
        with PartitionManager() as pm:
            random_sleep(3)
            node_to_drop_zk_connection = random_node()
            print(
                f"Dropping connection between {get_node_name(node_to_drop_zk_connection)} and ZooKeeper"
            )
            pm.drop_instance_zk_connections(node_to_drop_zk_connection)
            random_sleep(3)
            print(
                f"Restoring connection between {get_node_name(node_to_drop_zk_connection)} and ZooKeeper"
            )

        # Backup must be successful.
        wait_status(initiator, "BACKUP_CREATED", backup_id=backup_id)
        assert get_num_system_processes(nodes, backup_id=backup_id) == 0

        no_trash_checker.expect_backups = [backup_id]
        no_trash_checker.allow_errors = [
            "KEEPER_EXCEPTION",
            "SOCKET_TIMEOUT",
            "CANNOT_READ_ALL_DATA",
            "NETWORK_ERROR",
            "TABLE_IS_READ_ONLY",
        ]


# A restore must NOT be stopped if Zookeeper is disconnected shorter than `failure_after_host_disconnected_for_seconds`.
def test_short_disconnection_doesnt_stop_restore():
    # Make a backup.
    backup_id = get_backup_id_of_successful_backup()

    # Restore from the backup.
    with NoTrashChecker() as no_trash_checker, ConfigManager() as config_manager:
        use_faster_zk_disconnect_detect = random.choice([True, False])
        if use_faster_zk_disconnect_detect:
            print("Using faster_zk_disconnect_detect.xml")
            config_manager.add_main_config(
                nodes, "configs/faster_zk_disconnect_detect.xml"
            )

        initiator = random_node()
        print(f"Using {get_node_name(initiator)} as initiator")

        restore_id = random_id()
        initiator.query(
            f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC",
            settings={"backup_restore_failure_after_host_disconnected_for_seconds": 6},
        )

        assert get_status(initiator, restore_id=restore_id) == "RESTORING"
        assert get_num_system_processes(initiator, restore_id=restore_id) >= 1

        # Dropping connection for less than `failure_after_host_disconnected_for_seconds`
        with PartitionManager() as pm:
            random_sleep(3)
            node_to_drop_zk_connection = random_node()
            print(
                f"Dropping connection between {get_node_name(node_to_drop_zk_connection)} and ZooKeeper"
            )
            pm.drop_instance_zk_connections(node_to_drop_zk_connection)
            random_sleep(3)
            print(
                f"Restoring connection between {get_node_name(node_to_drop_zk_connection)} and ZooKeeper"
            )

        # Restore must be successful.
        wait_status(initiator, "RESTORED", restore_id=restore_id)
        assert get_num_system_processes(nodes, restore_id=restore_id) == 0

        no_trash_checker.allow_errors = [
            "KEEPER_EXCEPTION",
            "SOCKET_TIMEOUT",
            "CANNOT_READ_ALL_DATA",
            "NETWORK_ERROR",
            "TABLE_IS_READ_ONLY",
        ]
