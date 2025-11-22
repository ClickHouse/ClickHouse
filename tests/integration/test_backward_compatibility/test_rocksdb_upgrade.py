import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    node.restart_with_original_version(clear_data_dir=True)


def test_rocksdb_upgrade(start_cluster):
    node.query(
        """
               CREATE TABLE vt
               (
                   `sha256` FixedString(64),
                   `amount` Nullable(Int8)
               )
               ENGINE = EmbeddedRocksDB
               PRIMARY KEY sha256
               """
    )

    assert node.query("SELECT count() FROM vt") == "0\n"

    node.restart_with_latest_version()

    assert node.query("SELECT count() FROM vt") == "0\n"

    node.query("""DROP TABLE vt""")
