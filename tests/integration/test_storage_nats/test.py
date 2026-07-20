import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/named_collection.xml"],
    with_nats=True,
    stay_alive=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def read_metadata(metadata_path):
    return node.exec_in_container(
        [
            "/usr/bin/clickhouse",
            "disks",
            "-C",
            "/etc/clickhouse-server/config.xml",
            "--disk",
            get_database_disk_name(node),
            "--save-logs",
            "--query",
            f"read --path-from {metadata_path}",
        ]
    )


def test_named_collection_loads_legacy_empty_settings_metadata():
    node.query(
        """
        CREATE TABLE nats_empty_settings (x UInt8)
        ENGINE = NATS(
            nats_default,
            nats_subjects = 'issue_107750',
            nats_format = 'JSONEachRow'
        )
        """
    )

    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'default' AND name = 'nats_empty_settings'"
    ).strip()
    assert not read_metadata(metadata_path).rstrip().endswith("SETTINGS")

    node.stop_clickhouse()
    replace_text_in_metadata(
        node,
        metadata_path,
        "nats_format = 'JSONEachRow')",
        "nats_format = 'JSONEachRow')\nSETTINGS",
    )
    assert read_metadata(metadata_path).rstrip().endswith("SETTINGS")
    node.start_clickhouse()

    assert "NATS" in node.query("SHOW CREATE TABLE nats_empty_settings")
