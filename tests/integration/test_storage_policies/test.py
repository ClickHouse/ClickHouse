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


def test_alter_storage_policy_with_empty_contents(started_cluster):
    node.query(
        "CREATE TABLE test_empty_dir (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'disk1_only_policy'"
    )
    node.query("INSERT INTO test_empty_dir VALUES (1)")

    disk1_path = "/var/lib/clickhouse1/"
    data_path = node.query(
        "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'test_empty_dir'"
    ).strip()
    relative_data_path = data_path.removeprefix(disk1_path)

    disk2_data_path = f"/var/lib/clickhouse2/{relative_data_path}"
    quoted_disk2_data_path = shlex.quote(disk2_data_path)

    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {quoted_disk2_data_path}/detached "
            f"{quoted_disk2_data_path}/tmp_1_1_0 "
            f"{quoted_disk2_data_path}/tmp-fetch_1_1_0 && "
            f"echo 1 > {quoted_disk2_data_path}/format_version.txt",
        ]
    )

    node.query("ALTER TABLE test_empty_dir MODIFY SETTING storage_policy = 'test_policy'")
    assert (
        node.query(
            """
            SELECT count() > 0
            FROM system.storage_policies
            WHERE policy_name = (
                SELECT storage_policy
                FROM system.tables
                WHERE database = currentDatabase() AND name = 'test_empty_dir'
            ) AND has(disks, 'disk2')
            """
        )
        == "1\n"
    )
    assert node.query("SELECT * FROM test_empty_dir") == "1\n"

    node.query("DROP TABLE test_empty_dir")
