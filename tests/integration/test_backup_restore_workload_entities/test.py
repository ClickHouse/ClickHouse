import glob
import os.path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml", "configs/entities.xml"],
    external_dirs=["/backups/"],
)

# A second, replicated (Keeper-backed) cluster of two nodes sharing a single workload entity storage,
# used by the ON CLUSTER round-trip test below.
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/backups_disk.xml",
        "configs/cluster.xml",
        "configs/replicated_workloads.xml",
    ],
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/backups_disk.xml",
        "configs/cluster.xml",
        "configs/replicated_workloads.xml",
    ],
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_sql_entities():
    # Keep only the config-defined entities between tests; SQL entities are created per test.
    yield
    instance.query("DROP WORKLOAD IF EXISTS sql_wl")
    instance.query("DROP RESOURCE IF EXISTS sql_res")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def get_path_to_backup(backup_name):
    name = backup_name.split(",")[1].strip("')/ ")
    return os.path.join(instance.cluster.instances_dir, "backups", name)


def find_files_in_backup_folder(backup_name):
    path = get_path_to_backup(backup_name)
    files = [f for f in glob.glob(path + "/**", recursive=True) if os.path.isfile(f)]
    files += [f for f in glob.glob(path + "/.**", recursive=True) if os.path.isfile(f)]
    return files


def backed_up_entity_files(backup_name, entity_kind):
    # Names of the per-entity .sql files stored under data/system/<entity_kind>/ in the backup.
    files = find_files_in_backup_folder(backup_name)
    marker = f"/{entity_kind}/"
    return sorted(
        os.path.basename(f) for f in files if marker in f and f.endswith(".sql")
    )


def names(query_result):
    return sorted(line for line in query_result.strip().split("\n") if line)


def test_backup_excludes_config_defined_entities():
    # The config-defined entities are loaded from <resources_and_workloads> at startup.
    assert "cfg_wl" in names(instance.query("SELECT name FROM system.workloads"))
    assert "all" in names(instance.query("SELECT name FROM system.workloads"))
    assert "cfg_res" in names(instance.query("SELECT name FROM system.resources"))

    # Create entities via SQL (stored in the primary, writable storage).
    instance.query("CREATE RESOURCE sql_res (WRITE DISK sql_disk, READ DISK sql_disk)")
    instance.query("CREATE WORKLOAD sql_wl IN all SETTINGS priority = 3")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE system.workloads, TABLE system.resources TO {backup_name}"
    )

    # The backup must contain ONLY the SQL-defined entities, never the config-defined ones.
    assert backed_up_entity_files(backup_name, "workloads") == ["sql_wl.sql"]
    assert backed_up_entity_files(backup_name, "resources") == ["sql_res.sql"]

    # Drop the SQL entities and restore them from the backup.
    instance.query("DROP WORKLOAD sql_wl")
    instance.query("DROP RESOURCE sql_res")
    instance.query(
        f"RESTORE TABLE system.workloads, TABLE system.resources FROM {backup_name}"
    )

    # The SQL entities are restored ...
    workloads = names(instance.query("SELECT name FROM system.workloads"))
    resources = names(instance.query("SELECT name FROM system.resources"))
    assert "sql_wl" in workloads
    assert "sql_res" in resources

    # ... and the config-defined entities are still present exactly once
    # (not duplicated, not turned into SQL entities by the restore).
    assert workloads.count("all") == 1
    assert workloads.count("cfg_wl") == 1
    assert resources.count("cfg_res") == 1


def test_restore_rejects_entity_kind_mismatch():
    # restore() rejects a .sql file whose entity kind does not match the system table being restored
    # (a WORKLOAD definition under data/system/resources/, or a RESOURCE under data/system/workloads/).
    # That guard is what keeps the per-table CREATE_WORKLOAD / CREATE_RESOURCE access split safe -- a
    # WORKLOAD restored via system.resources would otherwise only require CREATE_RESOURCE. Emulate a
    # malformed backup by moving the backed-up WORKLOAD file into the resources data directory and
    # renaming its manifest entry to match, then assert RESTORE fails with CANNOT_RESTORE_TABLE.
    instance.query("CREATE RESOURCE sql_res (WRITE DISK sql_disk, READ DISK sql_disk)")
    instance.query("CREATE WORKLOAD sql_wl IN all SETTINGS priority = 3")

    backup_name = new_backup_name()
    instance.query(
        f"BACKUP TABLE system.workloads, TABLE system.resources TO {backup_name}"
    )
    assert backed_up_entity_files(backup_name, "workloads") == ["sql_wl.sql"]

    # Move the physical WORKLOAD file into the resources data directory (the directory exists because
    # sql_res was backed up there) ...
    src = next(
        f
        for f in find_files_in_backup_folder(backup_name)
        if f.endswith("/workloads/sql_wl.sql")
    )
    os.rename(src, src.replace("/workloads/sql_wl.sql", "/resources/sql_wl.sql"))
    # ... and point its manifest entry at the new path so RESTORE reads it under system.resources.
    meta_path = os.path.join(get_path_to_backup(backup_name), ".backup")
    with open(meta_path) as f:
        meta = f.read()
    with open(meta_path, "w") as f:
        f.write(meta.replace("/workloads/sql_wl.sql", "/resources/sql_wl.sql"))

    instance.query("DROP WORKLOAD sql_wl")
    instance.query("DROP RESOURCE sql_res")

    error = instance.query_and_get_error(
        f"RESTORE TABLE system.workloads, TABLE system.resources FROM {backup_name}"
    )
    assert "CANNOT_RESTORE_TABLE" in error


def test_backup_restore_on_cluster():
    # node1 and node2 share a single Keeper-backed workload entity storage (configs/replicated_workloads.xml),
    # so an entity created on one node replicates to the other. This exercises the ON CLUSTER coordination
    # (a single elected writer per replication id, other replicas picking the change up through replication)
    # that a single-node test cannot reach.
    node1.query("CREATE RESOURCE sql_res (WRITE DISK sql_disk, READ DISK sql_disk)")
    node1.query("CREATE WORKLOAD all")
    node1.query("CREATE WORKLOAD sql_wl IN all SETTINGS priority = 3")

    # The entities replicate to node2 through the shared storage.
    assert_eq_with_retry(
        node2, "SELECT name FROM system.workloads ORDER BY name", "all\nsql_wl\n"
    )
    assert_eq_with_retry(node2, "SELECT name FROM system.resources", "sql_res\n")

    backup_name = new_backup_name()
    node1.query(
        f"BACKUP TABLE system.workloads, TABLE system.resources ON CLUSTER 'workloads_cluster' TO {backup_name}"
    )

    # Drop every entity across the cluster; the drop replicates to node2 as well.
    node1.query("DROP WORKLOAD sql_wl")
    node1.query("DROP WORKLOAD all")
    node1.query("DROP RESOURCE sql_res")
    assert_eq_with_retry(node2, "SELECT count() FROM system.workloads", "0\n")
    assert_eq_with_retry(node2, "SELECT count() FROM system.resources", "0\n")

    node1.query(
        f"RESTORE TABLE system.workloads, TABLE system.resources ON CLUSTER 'workloads_cluster' FROM {backup_name}"
    )

    # The SQL entities are restored and visible on BOTH replicas.
    for node in (node1, node2):
        assert_eq_with_retry(
            node, "SELECT name FROM system.workloads ORDER BY name", "all\nsql_wl\n"
        )
        assert_eq_with_retry(node, "SELECT name FROM system.resources", "sql_res\n")

    # Clean up so module teardown (and any re-run) starts from empty storage.
    node1.query("DROP WORKLOAD IF EXISTS sql_wl")
    node1.query("DROP WORKLOAD IF EXISTS all")
    node1.query("DROP RESOURCE IF EXISTS sql_res")
