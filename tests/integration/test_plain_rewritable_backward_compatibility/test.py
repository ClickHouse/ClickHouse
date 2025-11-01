import logging
import pytest
import base64
import shlex

from minio.deleteobjects import DeleteObject
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

setup_settings = {
    'main_configs': ["configs/storage_conf.xml"],
    'with_minio': True,
    'stay_alive': True,
    'with_remote_database_disk': False,
}
node_25_6 = cluster.add_instance("node_25_6", image="clickhouse/clickhouse-server", tag="25.6", with_installed_binary=True, **setup_settings)
node_25_8 = cluster.add_instance("node_25_8", image="clickhouse/clickhouse-server", tag="25.8", with_installed_binary=True, **setup_settings)
node_25_10 = cluster.add_instance("node_25_10", image="clickhouse/clickhouse-server", tag="25.10", with_installed_binary=True, **setup_settings)
node_master = cluster.add_instance("node_master", **setup_settings)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        # We are making tmp table here to force creation of metadata symlink
        node_25_6.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_25_8.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_25_10.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_master.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")

        node_25_6.stop_clickhouse()
        node_25_8.stop_clickhouse()
        node_25_10.stop_clickhouse()
        node_master.stop_clickhouse()

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "storage_declaration, lost_blobs",
    [
        ("disk = 's3_plain_rewritable'", 0),
        ("table_disk = 1, disk = 's3_plain_rewritable'", 1),  # /data/format_version.txt
    ],
)
def test_backward_compatibility(start_cluster, storage_declaration, lost_blobs):
    node_25_6.start_clickhouse()
    node_25_6.query(f"CREATE TABLE mt (version String) ENGINE = MergeTree ORDER BY () SETTINGS {storage_declaration}")
    node_25_6.query("INSERT INTO mt VALUES ('25.6')")
    node_25_6.stop_clickhouse()

    table_attach_query = node_25_6.exec_in_container(["cat", "/var/lib/clickhouse/metadata/default/mt.sql"])
    table_attach_query_b64 = base64.b64encode(table_attach_query.encode()).decode()
    for node in [node_25_8, node_25_10, node_master]:
        node.exec_in_container(["bash", "-lc", f"printf %s {shlex.quote(table_attach_query_b64)} | base64 -d > /var/lib/clickhouse/metadata/default/mt.sql"])
        assert node.exec_in_container(["ls", "/var/lib/clickhouse/metadata/default"]).split() == ["mt.sql", "tmp.sql"]
        assert node.exec_in_container(["cat", "/var/lib/clickhouse/metadata/default/mt.sql"]) == table_attach_query

    node_25_8.start_clickhouse()
    assert node_25_8.query("SHOW TABLES").split() == ["mt", "tmp"]
    assert node_25_8.query("SELECT version FROM mt ORDER BY version").split() == ["25.6"]
    node_25_8.query("INSERT INTO mt VALUES ('25.8')")
    node_25_8.stop_clickhouse()

    node_25_10.start_clickhouse()
    assert node_25_10.query("SHOW TABLES").split() == ["mt", "tmp"]
    assert node_25_10.query("SELECT version FROM mt ORDER BY version").split() == ["25.6", "25.8"]
    node_25_10.query("INSERT INTO mt VALUES ('25.10')")
    node_25_10.stop_clickhouse()

    node_master.start_clickhouse()
    assert node_master.query("SHOW TABLES").split() == ["mt", "tmp"]
    assert node_master.query("SELECT version FROM mt ORDER BY version").split() == ["25.10", "25.6", "25.8"]
    node_master.query("INSERT INTO mt VALUES ('master')")
    node_master.stop_clickhouse()

    node_25_10.start_clickhouse()
    assert node_25_10.query("SELECT version FROM mt ORDER BY version").split() == ["25.10", "25.6", "25.8", "master"]
    node_25_10.stop_clickhouse()

    node_25_8.start_clickhouse()
    assert node_25_8.query("SELECT version FROM mt ORDER BY version").split() == ["25.10", "25.6", "25.8", "master"]
    node_25_8.stop_clickhouse()

    node_25_6.start_clickhouse()
    assert node_25_6.query("SELECT version FROM mt ORDER BY version").split() == ["25.10", "25.6", "25.8", "master"]
    node_25_6.stop_clickhouse()

    node_master.start_clickhouse()
    node_master.query("DROP TABLE mt SYNC")
    node_master.stop_clickhouse()

    for node in [node_25_6, node_25_8, node_25_10]:
        node.exec_in_container(["rm", "/var/lib/clickhouse/metadata/default/mt.sql"])

    blobs = [obj.object_name for obj in cluster.minio_client.list_objects(cluster.minio_bucket, 'data/', recursive=True)]
    assert len(blobs) == lost_blobs

    if len(blobs) != 0:
        to_remove = [DeleteObject(blob) for blob in blobs]
        errors = cluster.minio_client.remove_objects(cluster.minio_bucket, to_remove)
        for error in errors:
            logging.error(f"Error occurred when deleting object {error}")
