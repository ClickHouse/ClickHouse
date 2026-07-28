import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/database_disk.xml",
    ],
    with_remote_database_disk=False,
    with_minio = True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/database_disk.xml",
    ],
    with_remote_database_disk=False,
    with_minio=True,
    stay_alive=True,
)

disk_config_file_path =  "/tmp/disk_app_config.xml"

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node1.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/disk_app_config.xml"),
            disk_config_file_path,
        )
        node2.copy_file_to_container(
            os.path.join(SCRIPT_DIR, "configs/disk_app_config.xml"),
            disk_config_file_path,
        )
        yield cluster
    finally:
        cluster.shutdown()

def directory_exists(node, disk_name: str, dir_path: str):
    path = Path(dir_path.rstrip("/"))
    parent_dir = path.parent
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C /tmp/disk_app_config.xml --save-logs --disk {disk_name} --query "
    file_list = node.exec_in_container(
        [
            "bash",
            "-c",
            f"{disk_cmd_prefix} 'ls {parent_dir}'",
        ]
    )
    return path.name in file_list

def validate_table_metadata_path(node, disk_name : str, db : str, table: str):
    table_metadata_path = node1.query(
        f"SELECT metadata_path FROM system.tables WHERE database='{db}' AND table='{table}'"
    ).strip()

    assert directory_exists(node, disk_name, table_metadata_path)
    if(disk_name != "global_db_disk"):
        assert not directory_exists(node1, "global_db_disk", table_metadata_path)

def validate_db_path(node, disk_name : str, db : str, support_symlink: bool = True):
    db_data_path = node.query(
        f"SELECT metadata_path FROM system.databases WHERE database='{db}'"
    ).strip()

    assert directory_exists(node, disk_name, db_data_path)
    if support_symlink:
        assert directory_exists(node, disk_name, os.path.join("metadata", db))
        assert directory_exists(node, disk_name, os.path.join( "data", db))

@pytest.mark.parametrize("engine", ["Atomic", "Ordinary"])
@pytest.mark.parametrize("db_disk_name", ["db_disk", "global_db_disk", ""])
def test_db_disk_setting(start_cluster, engine: str, db_disk_name: str):
    db_name = f"db_{db_disk_name}_{engine.lower()}"

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    node1.query(f"DROP DATABASE IF EXISTS {db_name}_rename SYNC")

    disk_setting = f"disk='{db_disk_name}'"
    if len(db_disk_name) == 0:
        disk_setting = "disk=disk(type='local', path='/var/lib/clickhouse/disks/custom_db_disk/')"
        db_disk_name = "custom_db_disk"

    node1.query(
        sql=f"CREATE DATABASE {db_name} ENGINE= {engine} SETTINGS {disk_setting}",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node1.query(f"CREATE TABLE {db_name}.test (x INT) ENGINE=MergeTree ORDER BY x")

    validate_db_path(node1, db_disk_name, db_name)
    validate_table_metadata_path(node1, db_disk_name, db_name, 'test')

    # Ordinay DB doesn't support renaming
    if(engine != "Ordinary"):
        node1.query(f"RENAME DATABASE {db_name} TO {db_name}_rename")
        validate_db_path(node1, db_disk_name, f"{db_name}_rename")
        validate_table_metadata_path(node1, db_disk_name, f"{db_name}_rename", 'test')

        node1.query(f"RENAME DATABASE {db_name}_rename TO {db_name}")
        validate_db_path(node1, db_disk_name, db_name)
        validate_table_metadata_path(node1, db_disk_name, db_name, 'test')

    node1.query(f"RENAME TABLE {db_name}.test TO {db_name}.test_rename")
    validate_db_path(node1, db_disk_name, db_name)
    validate_table_metadata_path(node1, db_disk_name, db_name, 'test_rename')

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    node1.query(f"DROP DATABASE IF EXISTS {db_name}_rename SYNC")

def replace_text_in_metadata(node, disk_name: str, metadata_path: str, old_value: str, new_value: str):
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C {disk_config_file_path} --disk {disk_name} --save-logs --query "

    old_metadata = node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {metadata_path}'"]
    )

    new_metadata = old_metadata.replace(old_value, new_value)
    write_to_file(node, disk_name, metadata_path, new_metadata)

def read_file(node, disk_name: str, metadata_path: str):
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C {disk_config_file_path} --disk {disk_name} --save-logs --query "

    return node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'read --path-from {metadata_path}'"]
    )

def write_to_file(node, disk_name: str, file_path: str, content: str):
    # Escape backticks to avoid command substitution
    escaped_content = content.replace('"', r"\"").replace("`", r"\`")
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C {disk_config_file_path} --save-logs --disk {disk_name} --query "
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"""printf "%s" "{escaped_content}" | {disk_cmd_prefix} 'w --path-to {file_path}'""",
        ]
    )

def remove_file(node, disk_name: str, file_path: str):
    # Escape backticks to avoid command substitution
    disk_cmd_prefix = f"/usr/bin/clickhouse disks -C {disk_config_file_path} --save-logs --disk {disk_name} --query "
    node.exec_in_container(
        ["bash", "-c", f"{disk_cmd_prefix} 'remove {file_path}'"]
    )


def test_attach_db_from_readonly_remote_disk(start_cluster):
    db_name = f"db_test"

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")

    node1.query(
        f"CREATE DATABASE {db_name} ENGINE=Atomic SETTINGS disk='s3_plain_rewritable'"
    )
    db_uuid = node1.query(
        f"SELECT uuid FROM system.databases WHERE database='{db_name}'"
    ).strip()
    table_disk_setting = 'disk(type = "s3_plain_rewritable", endpoint = "http://minio1:9001/root/data/disks/disk_s3_plain_rewritable/", access_key_id="minio", secret_access_key = "ClickHouse_Minio_P@ssw0rd")'
    node1.query(
        f"CREATE TABLE {db_name}.test (x INT, y INT) ENGINE=MergeTree ORDER BY x SETTINGS disk={table_disk_setting}"
    )
    node1.query(f"INSERT INTO {db_name}.test VALUES (1, 1)")

    assert node1.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"
    assert (
        node1.query("SELECT count() FROM system.tables WHERE table='test'").strip()
        == "1"
    )

    node2.restart_clickhouse()
    readonly_disk_setting = 'disk(readonly = 1, type = "s3_plain_rewritable", endpoint = "http://minio1:9001/root/data/disks/disk_s3_plain_rewritable/", access_key_id="minio", secret_access_key = "ClickHouse_Minio_P@ssw0rd")'
    node2.query(
        f"ATTACH DATABASE {db_name} UUID '{db_uuid}' ENGINE=Atomic SETTINGS disk={readonly_disk_setting}"
    )
    assert (
        node2.query("SELECT count() FROM system.tables WHERE table='test'").strip()
        == "1"
    )
    assert node2.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"

    node2.query(f"DETACH DATABASE {db_name}")
    assert node1.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"

    node2.query(f"ATTACH DATABASE {db_name}")
    assert node1.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"
    assert node2.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"

    node2.query(f"DROP DATABASE {db_name} SYNC")
    assert node1.query(f"SELECT * FROM {db_name}.test") == "1\t1\n"

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
