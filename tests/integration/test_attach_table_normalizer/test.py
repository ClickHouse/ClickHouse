import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_attach_substr(started_cluster):
    # Initialize
    node.query("DROP TABLE IF EXISTS default.file")
    node.query(
        "CREATE TABLE default.file(`s` String, `n` UInt8) ENGINE = MergeTree PARTITION BY substring(s, 1, 2) ORDER BY n "
    )

    # Detach table file
    node.query("DETACH TABLE file")

    metadata_path = node.query(
        f"SELECT metadata_path FROM system.detached_tables WHERE database='default' AND table='file'"
    ).strip()

    # Replace substring to substr
    replace_text_in_metadata(node, metadata_path, "substring", "substr")

    # Attach table file
    node.query("ATTACH TABLE file")


def test_attach_substr_restart(started_cluster):
    # Initialize
    node.query("DROP TABLE IF EXISTS default.file SYNC")
    node.query(
        "CREATE TABLE default.file(`s` String, `n` UInt8) ENGINE = MergeTree PARTITION BY substring(s, 1, 2) ORDER BY n "
    )

    metadata_path = node.query(
        f"SELECT metadata_path FROM system.tables WHERE database='default' AND table='file'"
    ).strip()

    # Replace substring to substr
    replace_text_in_metadata(node, metadata_path, "substring", "substr")

    # Restart clickhouse
    node.restart_clickhouse(kill=True)
