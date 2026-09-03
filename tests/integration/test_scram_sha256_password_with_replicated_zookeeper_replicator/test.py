import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/user_directories.xml"],
    with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_scram_sha256_password_with_replicated_zookeeper_replicator(start_cluster):
    node.query("DROP USER IF EXISTS user_scram_sha256_password")
    node.query(
        "CREATE USER user_scram_sha256_password IDENTIFIED WITH SCRAM_SHA256_PASSWORD BY 'qwerty14'"
    )
    node.query("SELECT 14", user="user_scram_sha256_password", password="qwerty14")

    node.query("DROP USER IF EXISTS user_scram_sha256_password")
