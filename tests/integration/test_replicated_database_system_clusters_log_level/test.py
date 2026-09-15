import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_system_clusters_query(database):
    # The whole of `system.clusters` is generated, so the filter does not change
    # what the query does to a Replicated database whose replicas are missing;
    # it only keeps the assertion about the rows this database contributes
    # independent of the other clusters the instance may have in its config
    # (the CI logs export adds one, see tests/integration/helpers/ci_logs_export.py).
    answer, error = node.query_and_get_answer_with_error(
        f"SELECT count() > 0 FROM system.clusters WHERE cluster = '{database}'",
        settings={"send_logs_level": "warning"},
    )
    assert answer == "0\n"
    assert "DatabaseReplicated" not in error


def create_replicated_database(database, path):
    node.query(
        f"CREATE DATABASE {database} ENGINE = Replicated('{path}', 'shard1', 'replica1')"
    )
    return node.query(
        f"SELECT metadata_path FROM system.databases WHERE name = '{database}'"
    ).strip()


def cleanup_replicated_database(database, path, metadata_path):
    node.query(f"DETACH DATABASE {database} SYNC")
    if metadata_path:
        node.exec_in_container(["rm", "-f", metadata_path])

    cluster.get_kazoo_client("zoo1").delete(path, recursive=True)


def test_system_clusters_does_not_log_missing_replicas(started_cluster):
    # Converted from stateless test 04252_database_replicated_system_clusters_log_level.sh.
    database = "clusters_log_level_missing"
    path = "/test/clusters_log_level_missing"
    metadata_path = ""

    try:
        metadata_path = create_replicated_database(database, path)
        cluster.get_kazoo_client("zoo1").delete(f"{path}/replicas", recursive=True)
        run_system_clusters_query(database)
    finally:
        cleanup_replicated_database(database, path, metadata_path)


def test_system_clusters_does_not_log_no_active_replicas(started_cluster):
    # Converted from stateless test 04254_database_replicated_system_clusters_no_active_replicas.sh.
    database = "clusters_log_level_empty"
    path = "/test/clusters_log_level_empty"
    metadata_path = ""

    try:
        metadata_path = create_replicated_database(database, path)
        zk = cluster.get_kazoo_client("zoo1")
        zk.delete(f"{path}/replicas", recursive=True)
        zk.create(f"{path}/replicas")
        run_system_clusters_query(database)
    finally:
        cleanup_replicated_database(database, path, metadata_path)
