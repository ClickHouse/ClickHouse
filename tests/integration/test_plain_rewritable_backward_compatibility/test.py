import logging
import pytest
import base64
import shlex

from minio.deleteobjects import DeleteObject
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

cluster = ClickHouseCluster(__file__)

setup_settings = {
    'main_configs': ["configs/storage_conf.xml"],
    'user_configs': ["configs/compatibility.xml"],
    'with_minio': True,
    'stay_alive': True,
    'with_remote_database_disk': False,
}

main_configs_to_replace = {
    'main_configs' : ["configs/storage_conf.xml", "configs/remove_masking_rules.xml"]
}

node_25_4 = cluster.add_instance("node_25_4", image="clickhouse/clickhouse-server", tag="25.4", with_installed_binary=True, **setup_settings | main_configs_to_replace)
node_25_6 = cluster.add_instance("node_25_6", image="clickhouse/clickhouse-server", tag="25.6", with_installed_binary=True, **setup_settings | main_configs_to_replace)
node_25_8 = cluster.add_instance("node_25_8", image="clickhouse/clickhouse-server", tag="25.8", with_installed_binary=True, **setup_settings | main_configs_to_replace)
node_25_10 = cluster.add_instance("node_25_10", image="clickhouse/clickhouse-server", tag="25.10", with_installed_binary=True, **setup_settings | main_configs_to_replace)
node_master = cluster.add_instance("node_master", **setup_settings)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        # We are making tmp table here to force creation of metadata symlink
        node_25_4.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_25_6.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_25_8.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_25_10.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")
        node_master.query(f"CREATE TABLE tmp (version String) ENGINE = MergeTree ORDER BY ()")

        node_25_4.stop_clickhouse()
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


def test_backward_compatibility_readonly_tables(start_cluster):
    settings = "table_disk = 1, disk = 's3_plain_rewritable'"
    create_write_table_query = f"CREATE TABLE mt (version String) ENGINE = MergeTree ORDER BY () SETTINGS {settings}"
    create_readonly_table_query = f"CREATE TABLE mt (version String) ENGINE = MergeTree ORDER BY () settings {settings}, disk = disk(readonly = 1, type = 's3_plain_rewritable', endpoint = 'http://minio1:9001/root/data/', access_key_id='minio', secret_access_key='{minio_secret_key}')"

    node_25_6.start_clickhouse()
    node_25_6.query(create_write_table_query)
    node_25_6.query("INSERT INTO mt VALUES ('25.6')")
    assert node_25_6.query("SELECT version FROM mt").split() == ["25.6"]
    node_25_6.stop_clickhouse()

    node_25_8.start_clickhouse()
    node_25_8.query(create_readonly_table_query)
    assert node_25_8.query("SELECT version FROM mt").split() == ["25.6"]
    node_25_8.query("DROP TABLE mt SYNC")
    node_25_8.stop_clickhouse()

    node_25_10.start_clickhouse()
    node_25_10.query(create_readonly_table_query)
    assert node_25_10.query("SELECT version FROM mt").split() == ["25.6"]
    node_25_10.query("DROP TABLE mt SYNC")
    node_25_10.stop_clickhouse()

    node_master.start_clickhouse()
    node_master.query(create_readonly_table_query)
    assert node_master.query("SELECT version FROM mt").split() == ["25.6"]
    node_master.query("DROP TABLE mt SYNC")
    node_master.stop_clickhouse()

    node_25_6.start_clickhouse()
    assert node_25_6.query("SELECT version FROM mt").split() == ["25.6"]
    node_25_6.query("DROP TABLE mt SYNC")
    node_25_6.stop_clickhouse()

    blobs = [obj.object_name for obj in cluster.minio_client.list_objects(cluster.minio_bucket, 'data/', recursive=True)]
    print(blobs)
    if len(blobs) != 0:
        to_remove = [DeleteObject(blob) for blob in blobs]
        errors = cluster.minio_client.remove_objects(cluster.minio_bucket, to_remove)
        for error in errors:
            logging.error(f"Error occurred when deleting object {error}")

# https://github.com/ClickHouse/ClickHouse/pull/80393
def test_backward_compatibility_bug_80393(start_cluster):
    create_table_query = f"CREATE TABLE mt (version String, PROJECTION prj (SELECT version ORDER BY version)) ENGINE = MergeTree ORDER BY () SETTINGS disk = 's3_plain_rewritable', merge_tree_clear_old_temporary_directories_interval_seconds=0"

    node_25_4.start_clickhouse()
    node_25_4.query(create_table_query)
    node_25_4.query("INSERT INTO mt VALUES ('25.4')")
    table_uuid = node_25_4.query("SELECT uuid FROM system.tables WHERE table = 'mt'").strip()
    assert node_25_4.query("SELECT version FROM mt").split() == ["25.4"]
    node_25_4.stop_clickhouse()

    blobs = [obj.object_name for obj in cluster.minio_client.list_objects(cluster.minio_bucket, 'data/', recursive=True)]
    data = [cluster.minio_client.get_object(cluster.minio_bucket, blob).data for blob in blobs]
    print(*zip(blobs, data))
    assert (f"store/{table_uuid[:3]}/{table_uuid}/tmp_insert_all_1_1_0/prj.proj/").encode('ascii') in data

    table_attach_query = node_25_4.exec_in_container(["cat", "/var/lib/clickhouse/metadata/default/mt.sql"])
    table_attach_query_b64 = base64.b64encode(table_attach_query.encode()).decode()
    node_master.exec_in_container(["bash", "-lc", f"printf %s {shlex.quote(table_attach_query_b64)} | base64 -d > /var/lib/clickhouse/metadata/default/mt.sql"])
    assert node_master.exec_in_container(["ls", "/var/lib/clickhouse/metadata/default"]).split() == ["mt.sql", "tmp.sql"]
    assert node_master.exec_in_container(["cat", "/var/lib/clickhouse/metadata/default/mt.sql"]) == table_attach_query

    node_master.start_clickhouse()
    assert node_master.query("SHOW TABLES").split() == ["mt", "tmp"]
    assert node_master.query("SELECT version FROM mt ORDER BY version").split() == ["25.4"]
    node_master.query("DROP TABLE mt SYNC")
    node_master.stop_clickhouse()

    for node in [node_25_4]:
        node.exec_in_container(["rm", "/var/lib/clickhouse/metadata/default/mt.sql"])

    blobs = [obj.object_name for obj in cluster.minio_client.list_objects(cluster.minio_bucket, 'data/', recursive=True)]
    data = [cluster.minio_client.get_object(cluster.minio_bucket, blob).data for blob in blobs]
    print(*zip(blobs, data))
    assert (f"store/{table_uuid[:3]}/{table_uuid}/tmp_insert_all_1_1_0/prj.proj/").encode('ascii') not in data

    if len(blobs) != 0:
        to_remove = [DeleteObject(blob) for blob in blobs]
        errors = cluster.minio_client.remove_objects(cluster.minio_bucket, to_remove)
        for error in errors:
            logging.error(f"Error occurred when deleting object {error}")
