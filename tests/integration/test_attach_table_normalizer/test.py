import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def replace_substring_to_substr(node):
    node.exec_in_container(
        [
            "bash",
            "-c",
            "sed -i 's/substring/substr/g' /var/lib/clickhouse/metadata/default/file.sql",
        ],
        user="root",
    )


def test_attach_substr(started_cluster):
    # Initialize
    node.query("DROP TABLE IF EXISTS default.file")
    node.query(
        "CREATE TABLE default.file(`s` String, `n` UInt8) ENGINE = MergeTree PARTITION BY substring(s, 1, 2) ORDER BY n "
    )

    # Detach table file
    node.query("DETACH TABLE file")

    # Replace substring to substr
    replace_substring_to_substr(node)

    # Attach table file
    node.query("ATTACH TABLE file")


def test_attach_substr_restart(started_cluster):
    # Initialize
    node.query("DROP TABLE IF EXISTS default.file")
    node.query(
        "CREATE TABLE default.file(`s` String, `n` UInt8) ENGINE = MergeTree PARTITION BY substring(s, 1, 2) ORDER BY n "
    )

    # Replace substring to substr
    replace_substring_to_substr(node)

    # Restart clickhouse
    node.restart_clickhouse(kill=True)
