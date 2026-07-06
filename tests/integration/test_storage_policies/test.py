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


def restart_with_config(config_name):
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, config_name),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.start_clickhouse()


def reload_config(config_name):
    node.copy_file_to_container(
        os.path.join(CONFIG_DIR, config_name),
        "/etc/clickhouse-server/config.d/disks.xml",
    )
    node.query("SYSTEM RELOAD CONFIG")


def get_disk2_data_path(table_name):
    disk1_path = "/var/lib/clickhouse1/"
    data_path = node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '{table_name}'"
    ).strip()
    return f"/var/lib/clickhouse2/{data_path.removeprefix(disk1_path)}"


def test_storage_policy_configuration_change(started_cluster):
    try:
        node.query(
            "CREATE TABLE a (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
        )

        node.stop_clickhouse()
        node.copy_file_to_container(
            os.path.join(CONFIG_DIR, "disk2_only.xml"),
            "/etc/clickhouse-server/config.d/disks.xml",
        )
        node.start_clickhouse()
    finally:
        node.stop_clickhouse()
        node.copy_file_to_container(
            os.path.join(CONFIG_DIR, "disks.xml"),
            "/etc/clickhouse-server/config.d/disks.xml",
        )
        node.start_clickhouse()
        node.query("DROP TABLE IF EXISTS a")


def test_storage_policy_configuration_change_rejects_existing_disk_contents(started_cluster):
    restart_with_config("disk2_existing_before_reload.xml")

    table_name = "test_config_reload_existing_disk_contents"
    disk2_data_path = None
    try:
        node.query(
            f"CREATE TABLE {table_name} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
        )
        node.query(f"INSERT INTO {table_name} VALUES (1)")

        disk2_data_path = get_disk2_data_path(table_name)
        node.exec_in_container(
            ["bash", "-c", f"mkdir -p {shlex.quote(disk2_data_path)}/all_0_0_0"]
        )

        reload_config("disk2_added_to_test_policy.xml")

        assert (
            node.query(
                "SELECT max(has(disks, 'disk2')) FROM system.storage_policies WHERE policy_name = 'test_policy'"
            )
            == "1\n"
        )
        assert "already contain data" in node.query_and_get_error(
            f"INSERT INTO {table_name} VALUES (2)"
        )
    finally:
        try:
            if disk2_data_path:
                node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])
            node.query(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            restart_with_config("disks.xml")


def test_storage_policy_configuration_change_initializes_existing_disk_on_use(started_cluster):
    restart_with_config("disk2_existing_before_reload.xml")

    table_name = "test_config_reload_existing_disk_initialized_on_use"
    disk2_data_path = None
    try:
        node.query(
            f"CREATE TABLE {table_name} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
        )
        node.query(f"INSERT INTO {table_name} VALUES (1)")

        disk2_data_path = get_disk2_data_path(table_name)

        reload_config("disk2_added_to_test_policy.xml")

        node.query(f"INSERT INTO {table_name} VALUES (2)")
        assert node.query(f"SELECT sum(x) FROM {table_name}") == "3\n"
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"test -f {shlex.quote(disk2_data_path)}/table_identity.json",
            ]
        )
    finally:
        try:
            node.query(f"DROP TABLE IF EXISTS {table_name}")
            if disk2_data_path:
                node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])
        finally:
            restart_with_config("disks.xml")


def test_storage_policy_configuration_change_rejects_bad_table_identity(started_cluster):
    restart_with_config("disk2_existing_before_reload.xml")

    table_name = "test_config_reload_bad_table_identity"
    disk2_data_path = None
    try:
        node.query(
            f"CREATE TABLE {table_name} (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'test_policy'"
        )
        node.query(f"INSERT INTO {table_name} VALUES (1)")

        disk2_data_path = get_disk2_data_path(table_name)
        bad_identity = (
            '{"version":1,"table_uuid":null,'
            '"relative_data_path":"wrong/path/","format_version":1}'
        )
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p {shlex.quote(disk2_data_path)} && "
                f"printf '%s' {shlex.quote(bad_identity)} > "
                f"{shlex.quote(disk2_data_path)}/table_identity.json",
            ]
        )

        reload_config("disk2_added_to_test_policy.xml")

        assert "Bad table identity file" in node.query_and_get_error(
            f"INSERT INTO {table_name} VALUES (2)"
        )
    finally:
        try:
            if disk2_data_path:
                node.exec_in_container(["bash", "-c", f"rm -rf {shlex.quote(disk2_data_path)}"])
            node.query(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            restart_with_config("disks.xml")


def test_alter_storage_policy_with_existing_disk_contents(started_cluster):
    ignored_contents = (
        "mkdir -p {disk2_data_path}/detached "
        "{disk2_data_path}/detached/not_a_part "
        "{disk2_data_path}/tmp_1_1_0 "
        "{disk2_data_path}/delete_tmp_all_0_0_0 "
        "{disk2_data_path}/tmp-fetch_1_1_0 && "
        "cp {data_path}/format_version.txt {disk2_data_path}/format_version.txt"
    )

    def exec_sh(command):
        node.exec_in_container(["bash", "-c", command])

    def render(command, data_path, disk2_data_path):
        return command.format(data_path=shlex.quote(data_path), disk2_data_path=shlex.quote(disk2_data_path))

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

    def check_case(table_name, command, expected_error=None, with_ignored_contents=False):
        data_path, disk2_data_path = create_table(table_name)
        try:
            if with_ignored_contents:
                exec_sh(render(ignored_contents, data_path, disk2_data_path))
            if command:
                exec_sh(render(command, data_path, disk2_data_path))

            alter_query = f"ALTER TABLE {table_name} MODIFY SETTING storage_policy = 'test_policy'"
            if expected_error:
                assert expected_error in node.query_and_get_error(alter_query)
            else:
                node.query(alter_query)
                assert node.query(f"SELECT * FROM {table_name}") == "1\n"
        finally:
            node.query(f"DROP TABLE IF EXISTS {table_name}")
            exec_sh(f"rm -rf {shlex.quote(disk2_data_path)}")

    for table_name, command in [
        ("test_empty_directory", "mkdir -p {disk2_data_path}"),
        ("test_tmp_directory", "mkdir -p {disk2_data_path}/tmp_1_1_0"),
        ("test_delete_tmp_directory", "mkdir -p {disk2_data_path}/delete_tmp_all_0_0_0"),
        ("test_tmp_fetch_directory", "mkdir -p {disk2_data_path}/tmp-fetch_1_1_0"),
    ]:
        check_case(table_name, command)

    check_case("test_ignored_contents", None, with_ignored_contents=True)

    for table_name, command, expected_error, with_ignored_contents in [
        ("test_mismatched_format_version", "mkdir -p {disk2_data_path} && printf 255 > {disk2_data_path}/format_version.txt", "Version file", False),
        ("test_format_version_directory", "mkdir -p {disk2_data_path}/format_version.txt", "Bad version file", False),
        (
            "test_detached_file",
            "mkdir -p {disk2_data_path} && cp {data_path}/format_version.txt {disk2_data_path}/format_version.txt && touch {disk2_data_path}/detached",
            "already contain data",
            False,
        ),
        ("test_unknown_root_entry", "mkdir -p {disk2_data_path}/not_a_part", "already contain data", True),
        ("test_temporary_file", "touch {disk2_data_path}/tmp_not_a_directory", "already contain data", True),
        ("test_valid_root_part", "mkdir -p {disk2_data_path}/all_0_0_0", "already contain data", True),
        ("test_valid_detached_part", "mkdir -p {disk2_data_path}/detached/all_0_0_0", "already contain data", True),
    ]:
        check_case(table_name, command, expected_error, with_ignored_contents=with_ignored_contents)
