import io
import json
import logging
import time
import uuid

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key

MINIO_INTERNAL_PORT = 9001


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s3_node",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
                "configs/config.d/cluster.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
            with_minio=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_s3_collection(node, name, minio_host, bucket, path="", extra_params=""):
    node.query(f"DROP NAMED COLLECTION IF EXISTS {name}")
    url = f"http://{minio_host}:{MINIO_INTERNAL_PORT}/{bucket}/{path}"
    node.query(f"""
        CREATE NAMED COLLECTION {name} AS
        url = '{url}',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}'
        {extra_params}
    """)


def test_s3_table_function_with_overrides(cluster):
    node = cluster.instances["s3_node"]
    node.query("DROP NAMED COLLECTION IF EXISTS s3_override")
    node.query(f"""
        CREATE NAMED COLLECTION s3_override AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/',
        access_key_id = 'minio' NOT OVERRIDABLE,
        secret_access_key = '{minio_secret_key}' NOT OVERRIDABLE,
        format = 'CSV'
    """)

    node.query("""
        INSERT INTO FUNCTION s3(s3_override, filename='override_test.csv', structure='x UInt32, y UInt32')
        VALUES (10, 20), (30, 40)
    """)

    result = node.query("""
        SELECT sum(x), sum(y) FROM s3(s3_override, filename='override_test.csv', structure='x UInt32, y UInt32')
    """).strip()
    assert result == "40\t60"

    node.query("DROP NAMED COLLECTION s3_override")


def test_s3_multiple_formats(cluster):
    node = cluster.instances["s3_node"]

    for format_name, ext in [("CSV", "csv"), ("JSONEachRow", "json"), ("TabSeparated", "tsv")]:
        coll_name = f"s3_{ext}"
        node.query(f"DROP NAMED COLLECTION IF EXISTS {coll_name}")
        node.query(f"""
            CREATE NAMED COLLECTION {coll_name} AS
            url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/format_test/',
            access_key_id = 'minio',
            secret_access_key = '{minio_secret_key}',
            format = '{format_name}'
        """)

        node.query(f"""
            INSERT INTO FUNCTION s3({coll_name}, filename='test.{ext}', structure='num UInt32')
            VALUES (100), (200), (300)
        """)

        result = node.query(f"SELECT sum(num) FROM s3({coll_name}, filename='test.{ext}', structure='num UInt32')").strip()
        assert result == "600", f"Failed for {format_name}"

        node.query(f"DROP NAMED COLLECTION {coll_name}")


def test_s3cluster_with_full_collection(cluster):
    node = cluster.instances["s3_node"]
    test_path = f"s3cluster_full_{uuid.uuid4().hex[:8]}"

    node.query("DROP NAMED COLLECTION IF EXISTS s3_full")
    node.query(f"""
        CREATE NAMED COLLECTION s3_full AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/{test_path}/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'JSONEachRow'
    """)

    node.query("""
        INSERT INTO FUNCTION s3(s3_full, filename='users.json', structure='id UInt64, name String, score Float64')
        VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3), (3, 'Charlie', 92.1)
    """)

    result = node.query("""
        SELECT count(*) FROM s3Cluster('cluster_simple', s3_full,
            filename = 'users.json', structure = 'id UInt64, name String, score Float64')
    """).strip()
    assert result == "3"

    result = node.query("""
        SELECT name FROM s3Cluster('cluster_simple', s3_full,
            filename = 'users.json', structure = 'id UInt64, name String, score Float64')
        WHERE score > 90 ORDER BY name
    """).strip()
    assert "Alice" in result and "Charlie" in result and "Bob" not in result

    node.query("DROP NAMED COLLECTION s3_full")


def test_s3cluster_glob_pattern(cluster):
    node = cluster.instances["s3_node"]
    test_path = f"s3cluster_glob_{uuid.uuid4().hex[:8]}"

    create_s3_collection(node, "s3_glob", cluster.minio_host, cluster.minio_bucket, f"{test_path}/")

    for i in range(3):
        node.query(f"""
            INSERT INTO FUNCTION s3(s3_glob, filename='part_{i}.csv', format='CSV', structure='x UInt32')
            VALUES ({i * 10}), ({i * 10 + 1}), ({i * 10 + 2})
        """)

    result = node.query("""
        SELECT sum(x) FROM s3Cluster('cluster_simple', s3_glob,
            filename = 'part_*.csv', format = 'CSV', structure = 'x UInt32')
    """).strip()
    assert result == "99"

    node.query("DROP NAMED COLLECTION s3_glob")


def test_s3_backup_restore(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP TABLE IF EXISTS backup_src")
    node.query("CREATE TABLE backup_src (id UInt64, data String) ENGINE = MergeTree() ORDER BY id")
    node.query("INSERT INTO backup_src SELECT number, toString(number * 10) FROM numbers(100)")

    create_s3_collection(node, "s3_backup", cluster.minio_host, cluster.minio_bucket, "backups/")

    backup_name = f"backup_{uuid.uuid4().hex[:8]}"
    node.query(f"BACKUP TABLE backup_src TO S3(s3_backup, '{backup_name}')")
    node.query("DROP TABLE backup_src")
    node.query(f"RESTORE TABLE backup_src FROM S3(s3_backup, '{backup_name}')")

    assert node.query("SELECT count() FROM backup_src").strip() == "100"
    assert node.query("SELECT data FROM backup_src WHERE id = 50").strip() == "500"

    node.query("DROP TABLE backup_src")
    node.query("DROP NAMED COLLECTION s3_backup")


def test_s3_incremental_backup(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP TABLE IF EXISTS inc_backup_test")
    node.query("CREATE TABLE inc_backup_test (id UInt64, val String) ENGINE = MergeTree() ORDER BY id")
    node.query("INSERT INTO inc_backup_test VALUES (1, 'initial')")

    create_s3_collection(node, "s3_inc", cluster.minio_host, cluster.minio_bucket, "inc_backups/")

    base_name = f"base_{uuid.uuid4().hex[:8]}"
    node.query(f"BACKUP TABLE inc_backup_test TO S3(s3_inc, '{base_name}')")

    node.query("INSERT INTO inc_backup_test VALUES (2, 'added'), (3, 'more')")

    inc_name = f"inc_{uuid.uuid4().hex[:8]}"
    node.query(f"""
        BACKUP TABLE inc_backup_test TO S3(s3_inc, '{inc_name}')
        SETTINGS base_backup = S3(s3_inc, '{base_name}')
    """)

    node.query("DROP TABLE inc_backup_test")
    node.query(f"RESTORE TABLE inc_backup_test FROM S3(s3_inc, '{inc_name}')")

    assert node.query("SELECT count() FROM inc_backup_test").strip() == "3"

    node.query("DROP TABLE inc_backup_test")
    node.query("DROP NAMED COLLECTION s3_inc")


def test_s3queue_after_processing_delete(cluster):
    node = cluster.instances["s3_node"]
    bucket = cluster.minio_bucket
    files_path = f"s3queue_del_{uuid.uuid4().hex[:8]}"
    keeper_path = f"/clickhouse/s3queue_del_{uuid.uuid4().hex[:8]}"

    create_s3_collection(node, "s3queue_del", cluster.minio_host, bucket, f"{files_path}/")

    node.query("DROP TABLE IF EXISTS s3queue_del_dst")
    node.query("CREATE TABLE s3queue_del_dst (x UInt32, y UInt32) ENGINE = MergeTree() ORDER BY x")

    node.query("DROP TABLE IF EXISTS s3queue_del_src")
    node.query(f"""
        CREATE TABLE s3queue_del_src (x UInt32, y UInt32)
        ENGINE = S3Queue(s3queue_del, filename='*.csv', format='CSV')
        SETTINGS mode = 'unordered', keeper_path = '{keeper_path}', after_processing = 'delete'
    """)

    node.query("DROP TABLE IF EXISTS s3queue_del_mv")
    node.query("CREATE MATERIALIZED VIEW s3queue_del_mv TO s3queue_del_dst AS SELECT * FROM s3queue_del_src")

    csv = "10,20\n30,40\n50,60\n"
    cluster.minio_client.put_object(bucket, f"{files_path}/to_delete.csv", io.BytesIO(csv.encode()), len(csv))

    for _ in range(30):
        if int(node.query("SELECT count() FROM s3queue_del_dst").strip()) == 3:
            break
        time.sleep(1)

    assert node.query("SELECT sum(x), sum(y) FROM s3queue_del_dst").strip() == "90\t120"

    time.sleep(2)
    objects = list(cluster.minio_client.list_objects(bucket, prefix=f"{files_path}/"))
    assert len(objects) == 0

    node.query("DROP TABLE IF EXISTS s3queue_del_mv")
    node.query("DROP TABLE IF EXISTS s3queue_del_src")
    node.query("DROP TABLE IF EXISTS s3queue_del_dst")
    node.query("DROP NAMED COLLECTION s3queue_del")


def test_s3_with_compression(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP NAMED COLLECTION IF EXISTS s3_gzip")
    node.query(f"""
        CREATE NAMED COLLECTION s3_gzip AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/compressed/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'CSV',
        compression = 'gzip'
    """)

    node.query("INSERT INTO FUNCTION s3(s3_gzip, filename='data.csv.gz', structure='val UInt64') SELECT number FROM numbers(1000)")

    result = node.query("SELECT sum(val) FROM s3(s3_gzip, filename='data.csv.gz', structure='val UInt64')").strip()
    assert result == "499500"

    node.query("DROP NAMED COLLECTION s3_gzip")


def test_s3_with_schema_in_collection(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP NAMED COLLECTION IF EXISTS s3_schema")
    node.query(f"""
        CREATE NAMED COLLECTION s3_schema AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/schema_test/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'TabSeparated',
        structure = 'id UInt64, name String, score Float32'
    """)

    node.query("INSERT INTO FUNCTION s3(s3_schema, filename='scores.tsv') VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3), (3, 'Charlie', 92.1)")

    result = float(node.query("SELECT avg(score) FROM s3(s3_schema, filename='scores.tsv')").strip())
    assert abs(result - 91.63) < 0.1

    node.query("DROP NAMED COLLECTION s3_schema")


# Multiple tables sharing same S3 credentials
def test_shared_credentials_multiple_tables(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP NAMED COLLECTION IF EXISTS shared_s3")
    node.query(f"""
        CREATE NAMED COLLECTION shared_s3 AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/shared/',
        access_key_id = 'minio' NOT OVERRIDABLE,
        secret_access_key = '{minio_secret_key}' NOT OVERRIDABLE
    """)

    # Create multiple tables using same credentials
    for name in ["users", "orders", "products"]:
        node.query(f"DROP TABLE IF EXISTS {name}_s3")
        node.query(f"""
            CREATE TABLE {name}_s3 (id UInt32, data String)
            ENGINE = S3(shared_s3, filename='{name}.csv', format='CSV')
        """)
        node.query(f"INSERT INTO {name}_s3 VALUES (1, '{name}_data')")

    # Verify all tables work
    assert node.query("SELECT data FROM users_s3").strip() == "users_data"
    assert node.query("SELECT data FROM orders_s3").strip() == "orders_data"
    assert node.query("SELECT data FROM products_s3").strip() == "products_data"

    for name in ["users", "orders", "products"]:
        node.query(f"DROP TABLE {name}_s3")
    node.query("DROP NAMED COLLECTION shared_s3")
