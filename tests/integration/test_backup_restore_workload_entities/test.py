import glob
import os.path

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml", "configs/entities.xml"],
    external_dirs=["/backups/"],
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
