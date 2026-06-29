import os
import shlex

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/disks.xml"], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_storage_policy_configuration_change(started_cluster):
    node.query(
        "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
    )

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disk2_only.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()

    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, "disks.xml"),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()


def test_alter_storage_policy_with_existing_disk_contents(started_cluster):
    def create_table(table_name):
        node.query(
            f"CREATE TABLE {table_name} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'disk1_only_policy'"
        )
        node.query(f"INSERT INTO {table_name} VALUES (1)")

        disk1_path = "/var/lib/clickhouse1/"
        data_path = node.query(
            f"SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'"
        ).strip()
        disk2_data_path = f"/var/lib/clickhouse2/{data_path.removeprefix(disk1_path)}"
        return data_path, disk2_data_path

    def create_ignored_contents(data_path, disk2_data_path):
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {shlex.quote(disk2_data_path)}/detached "
                f"{shlex.quote(disk2_data_path)}/detached/not_a_part "
                f"{shlex.quote(disk2_data_path)}/tmp_1_1_0 "
                f"{shlex.quote(disk2_data_path)}/delete_tmp_all_0_0_0 "
                f"{shlex.quote(disk2_data_path)}/tmp-fetch_1_1_0 && "
                f"cp {shlex.quote(data_path)}/format_version.txt {shlex.quote(disk2_data_path)}/format_version.txt",
            ]
        )

    table_name = "test_ignored_contents"
    data_path, disk2_data_path = create_table(table_name)
    try:
        create_ignored_contents(data_path, disk2_data_path)
        node.query(f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'")
        assert node.query("SELECT max(has(disks, 'disk2')) FROM system.storage_policies WHERE policy_name = 'test_policy'") == "1\n"
        assert node.query(f"SELECT * FROM {table_name}") == "1\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])

    table_name = "test_mismatched_format_version"
    _, disk2_data_path = create_table(table_name)
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {shlex.quote(disk2_data_path)} && "
                f"printf 255 > {shlex.quote(disk2_data_path)}/format_version.txt",
            ]
        )
        assert "Version file" in node.query_and_get_error(
            f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])

    table_name = "test_format_version_directory"
    _, disk2_data_path = create_table(table_name)
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {shlex.quote(disk2_data_path)}/format_version.txt",
            ]
        )
        assert "Bad version file" in node.query_and_get_error(
            f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])

    table_name = "test_detached_file"
    data_path, disk2_data_path = create_table(table_name)
    try:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {shlex.quote(disk2_data_path)} && "
                f"cp {shlex.quote(data_path)}/format_version.txt {shlex.quote(disk2_data_path)}/format_version.txt && "
                f"touch {shlex.quote(disk2_data_path)}/detached",
            ]
        )
        assert "already contain data" in node.query_and_get_error(
            f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'"
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {table_name}")
        node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])

    for table_name, part_path, create_path_command in [
        ("test_unknown_root_entry", "not_a_part", "mkdir -p"),
        ("test_temporary_file", "tmp_not_a_directory", "touch"),
        ("test_valid_root_part", "all_0_0_0", "mkdir -p"),
        ("test_valid_detached_part", "detached/all_0_0_0", "mkdir -p"),
    ]:
        data_path, disk2_data_path = create_table(table_name)
        try:
            create_ignored_contents(data_path, disk2_data_path)
            node.exec_in_container(
                ["bash", "-c", f"{create_path_command} {shlex.quote(disk2_data_path)}/{part_path}"]
            )
            assert "already contain data" in node.query_and_get_error(
                f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'"
            )
        finally:
            node.query(f"DROP TABLE IF EXISTS {table_name}")
            node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])
