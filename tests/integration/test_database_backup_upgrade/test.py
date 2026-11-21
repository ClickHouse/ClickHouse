import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def old_node(name):
    return cluster.add_instance(
        name,
        image="clickhouse/clickhouse-server",
        tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
        macros={"replica": name},
        with_installed_binary=True,
        stay_alive=True,
        with_zookeeper=True,
        use_keeper=False,
        with_remote_database_disk=False,
        external_dirs=["/backups/"],
        main_configs=["configs/backup_cluster.xml"],
    )


node1 = old_node("node1")
node2 = old_node("node2")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_legacy_ambiguous_backup_database_survives_upgrade(started_cluster):
    for node, marker in ((node1, 11), (node2, 22)):
        node.query("CREATE DATABASE source")
        node.query("CREATE TABLE source.events (id UInt64) ENGINE = MergeTree ORDER BY id")
        node.query(f"INSERT INTO source.events VALUES ({marker})")

    node1.query("BACKUP DATABASE source ON CLUSTER backup_upgrade TO Disk('backups', 'upgrade')")
    node1.query("BACKUP DATABASE source ON CLUSTER backup_upgrade TO Disk('backups', 'incremental') SETTINGS base_backup = Disk('backups', 'upgrade')")
    for node in (node1, node2):
        node.query("CREATE TABLE source.replicated_events (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/backup_upgrade/replicated_events', '{replica}') ORDER BY id")
    node1.query("INSERT INTO source.replicated_events VALUES (33)")
    node2.query("SYSTEM SYNC REPLICA source.replicated_events")
    node1.query("BACKUP DATABASE source ON CLUSTER backup_upgrade_replicas TO Disk('backups', 'replicas')")
    node1.query("CREATE DATABASE legacy ENGINE = Backup('source', Disk('backups', 'upgrade'))")
    metadata_path = "/var/lib/clickhouse/metadata/legacy.sql"
    definition = node1.exec_in_container(["cat", metadata_path])
    assert node1.query("SELECT count() FROM system.tables WHERE database = 'legacy'") == "0\n"

    node1.restart_with_latest_version()
    assert node1.query("SELECT 1") == "1\n"
    assert node1.exec_in_container(["cat", metadata_path]) == definition
    assert node1.query("SELECT count() FROM system.tables WHERE database = 'legacy'") == "0\n"
    assert node1.query("SELECT id FROM source.events") == "11\n"
    assert node1.contains_in_log("shard_num_in_backup")

    error = node1.query_and_get_error("CREATE DATABASE ambiguous ENGINE = Backup('source', Disk('backups', 'upgrade'))")
    assert "BAD_ARGUMENTS" in error
    assert "shard_num_in_backup" in error

    before = node1.exec_in_container(["cat", metadata_path])
    node1.query("DETACH DATABASE legacy")
    error = node1.query_and_get_error("ATTACH DATABASE legacy")
    assert "BAD_ARGUMENTS" in error
    assert "shard_num_in_backup" in error
    assert node1.exec_in_container(["cat", metadata_path]) == before

    node1.query("CREATE DATABASE selected_one ENGINE = Backup('source', Disk('backups', 'upgrade')) SETTINGS shard_num_in_backup = 1, replica_num_in_backup = 1")
    node1.query("CREATE DATABASE selected ENGINE = Backup('source', Disk('backups', 'upgrade')) SETTINGS shard_num_in_backup = 2, replica_num_in_backup = 1")
    assert node1.query("SELECT id FROM selected_one.events") == "11\n"
    assert node1.query("SELECT id FROM selected.events") == "22\n"
    node1.query("DROP DATABASE selected_one")
    node1.query("CREATE DATABASE selected_one ENGINE = Backup('source', Disk('backups', 'upgrade')) SETTINGS shard_num_in_backup = 2, replica_num_in_backup = 1")
    assert node1.query("SELECT id FROM selected_one.events") == "22\n"
    node1.query("DETACH DATABASE selected")
    node1.query("ATTACH DATABASE selected ENGINE = Backup('source', Disk('backups', 'upgrade')) SETTINGS shard_num_in_backup = 2, replica_num_in_backup = 1")
    assert node1.query("SELECT id FROM selected.events") == "22\n"
    node1.query("ALTER DATABASE selected MODIFY COMMENT 'selected source'")
    selected_definition = node1.query("SHOW CREATE DATABASE selected")
    assert "shard_num_in_backup = 2" in selected_definition
    assert "replica_num_in_backup = 1" in selected_definition
    selected_metadata_path = "/var/lib/clickhouse/metadata/selected.sql"
    selected_metadata = node1.exec_in_container(["cat", selected_metadata_path])
    node1.query("DETACH DATABASE selected")
    error = node1.query_and_get_error("ATTACH DATABASE selected ENGINE = Backup('source', Disk('backups', 'upgrade')) SETTINGS shard_num_in_backup = 1, replica_num_in_backup = 1")
    assert "BAD_ARGUMENTS" in error
    assert node1.exec_in_container(["cat", selected_metadata_path]) == selected_metadata
    node1.query("ATTACH DATABASE selected")
    assert node1.query("SELECT id FROM selected.events") == "22\n"

    error = node1.query_and_get_error("CREATE DATABASE ambiguous_replica ENGINE = Backup('source', Disk('backups', 'replicas')) SETTINGS shard_num_in_backup = 1")
    assert "BAD_ARGUMENTS" in error and "replica_num_in_backup" in error
    node1.query("CREATE DATABASE selected_replica ENGINE = Backup('source', Disk('backups', 'replicas')) SETTINGS shard_num_in_backup = 1, replica_num_in_backup = 2")
    assert node1.query("SELECT id FROM selected_replica.events") == "22\n"
    assert node1.query("SELECT id FROM selected_replica.replicated_events") == "33\n"
    node1.query("CREATE DATABASE selected_incremental ENGINE = Backup('source', Disk('backups', 'incremental')) SETTINGS shard_num_in_backup = 2, replica_num_in_backup = 1")
    assert node1.query("SELECT id FROM selected_incremental.events") == "22\n"

    node1.restart_clickhouse()
    assert node1.query("SHOW CREATE DATABASE selected") == selected_definition
    assert node1.query("SELECT id FROM selected.events") == "22\n"
