
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/remote_servers.xml",
    "configs/backups_disk.xml",
    "configs/keeper_map_path_prefix.xml",
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
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
    stay_alive=True,
)


node3 = cluster.add_instance(
    "node3",
    main_configs=main_configs,
    user_configs=user_configs,
    external_dirs=["/backups/"],
    macros={"replica": "node3", "shard": "shard2"},
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


backup_id_counter = 0


def new_backup_name(base_name):
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{base_name}{backup_id_counter}')"


def get_file_syncs(node):
    """Read this node's process-wide FileSync counter.

    `system.events` exposes `ProfileEvents::global_counters` of the queried server process only, so
    this reads one host at a time. `sum()` is used because the row is absent while the counter is 0.
    """
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'FileSync'"
        ).strip()
    )


@pytest.mark.parametrize("deduplicate_files", [0, 1])
def test_on_cluster(deduplicate_files):
    database_name = f"keeper_backup{deduplicate_files}"
    node1.query_with_retry(f"CREATE DATABASE {database_name} ON CLUSTER cluster")
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper1 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster1') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper2 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster1') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"CREATE TABLE {database_name}.keeper3 ON CLUSTER cluster (key UInt64, value String) Engine=KeeperMap('/{database_name}/test_on_cluster2') PRIMARY KEY key"
    )
    node1.query_with_retry(
        f"INSERT INTO {database_name}.keeper2 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5"
    )
    node1.query_with_retry(
        f"INSERT INTO {database_name}.keeper3 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5"
    )

    expected_result = "".join(f"{i}\ttest{i}\n" for i in range(5))

    def verify_data():
        for node in [node1, node2, node3]:
            for i in range(1, 4):
                result = node.query_with_retry(
                    f"SELECT key, value FROM {database_name}.keeper{i} ORDER BY key FORMAT TSV"
                )
                assert result == expected_result

    verify_data()

    backup_name = new_backup_name("test_on_cluster")
    # keeper1 and keeper2 share one zk root, and only one table may own the data of a root, so the
    # other one is collected as a BackupEntryReference - the only producer of one in the tree. With
    # deduplicate_files = 0 a reference is written as a copy of its target's data file, and each of
    # those copies has to be made durable too, which is what fsync_backup_files = 1 asserts here.
    # deduplicate_files = 1 is the control: it stores the duplicate once instead of copying it, so
    # it has no copies at all and a smaller num_entries.
    #
    # The count is summed over the cluster rather than read from the initiator's own row: each host
    # syncs the data files it wrote itself, the initiator's own operation writes only the manifest,
    # and which host ends up owning a shared zk root is decided by coordination, so no single host
    # has a stable count. Summed, the relation is exact - one fsync per entry actually written
    # (targets and copies alike, a plain backup keeping every file info as its own entry) plus the
    # one .backup manifest the initiator syncs last in finalizeWriting.
    backup_query_id = f"backup_{database_name}"
    nodes = [node1, node2, node3]
    file_syncs_before = sum(get_file_syncs(node) for node in nodes)
    node1.query(
        f"BACKUP DATABASE {database_name} ON CLUSTER cluster TO {backup_name} SETTINGS async = false, deduplicate_files = {deduplicate_files}, fsync_backup_files = 1;",
        query_id=backup_query_id,
    )
    file_syncs = sum(get_file_syncs(node) for node in nodes) - file_syncs_before

    # system.backups is in-memory and holds the cluster-wide counters, which BackupImpl recomputes
    # over the file infos of all hosts while writing the manifest. Waiting for the BACKUP_CREATED
    # row rather than reading whatever is there keeps a partially filled row out of the assertion.
    num_entries = int(
        wait_condition(
            lambda: node1.query(
                f"SELECT num_entries FROM system.backups"
                f" WHERE query_id = '{backup_query_id}' AND status = 'BACKUP_CREATED'"
            ).strip(),
            lambda s: s != "",
            max_attempts=60,
            delay=0.5,
        )
    )
    assert file_syncs == num_entries + 1, (
        f"expected {num_entries + 1} fsyncs across the cluster"
        f" ({num_entries} entries + the manifest), got {file_syncs}"
    )

    node1.query(f"DROP DATABASE {database_name} ON CLUSTER cluster SYNC;")

    def apply_for_all_nodes(f):
        for node in [node1, node2, node3]:
            f(node)

    def change_keeper_map_prefix(node):
        node.replace_config(
            "/etc/clickhouse-server/config.d/keeper_map_path_prefix.xml",
            """
<clickhouse>
    <keeper_map_path_prefix>/different_path/keeper_map</keeper_map_path_prefix>
</clickhouse>
""",
        )

    apply_for_all_nodes(lambda node: node.stop_clickhouse())
    apply_for_all_nodes(change_keeper_map_prefix)
    apply_for_all_nodes(lambda node: node.start_clickhouse())

    node1.query(
        f"RESTORE DATABASE {database_name} ON CLUSTER cluster FROM {backup_name} SETTINGS async = false;"
    )

    verify_data()

    node1.query(f"DROP TABLE {database_name}.keeper3 ON CLUSTER cluster SYNC;")
    node1.query(
        f"RESTORE TABLE {database_name}.keeper3 ON CLUSTER cluster FROM {backup_name} SETTINGS async = false;"
    )

    verify_data()
