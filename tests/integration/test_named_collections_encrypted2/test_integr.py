import io
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
            with_nginx=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_named_collections(cluster):
    """Drop all named collections before each test."""
    node = cluster.instances["s3_node"]
    collections = node.query("SELECT name FROM system.named_collections").strip()
    if collections:
        for coll in collections.split('\n'):
            if coll:
                node.query(f"DROP NAMED COLLECTION IF EXISTS {coll}")
    yield


def create_s3_collection(node, name, minio_host, bucket, path="", extra_params=""):
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
    node.query(f"""
        CREATE NAMED COLLECTION s3_override AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/',
        access_key_id = 'minio' NOT OVERRIDABLE,
        secret_access_key = '{minio_secret_key}' NOT OVERRIDABLE,
        format = 'CSV'
    """)

    node.query(
        """
        INSERT INTO FUNCTION s3(s3_override, filename = 'override_test.csv', structure = 'x UInt32, y UInt32')
        VALUES (10, 20),
               (30, 40)
        """)

    result = node.query(
        """
        SELECT sum(x), sum(y)
        FROM s3(s3_override, filename = 'override_test.csv', structure = 'x UInt32, y UInt32')
        """).strip()
    assert result == "40\t60"


def test_s3_multiple_formats(cluster):
    node = cluster.instances["s3_node"]

    for format_name, ext in [("CSV", "csv"), ("JSONEachRow", "json"), ("TabSeparated", "tsv")]:
        coll_name = f"s3_{ext}"
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

        result = node.query(
            f"SELECT sum(num) FROM s3({coll_name}, filename='test.{ext}', structure='num UInt32')").strip()
        assert result == "600", f"Failed for {format_name}"


def test_s3cluster_with_full_collection(cluster):
    node = cluster.instances["s3_node"]
    test_path = f"s3cluster_full_{uuid.uuid4().hex[:8]}"

    node.query(f"""
        CREATE NAMED COLLECTION s3_full AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/{test_path}/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'JSONEachRow'
    """)

    node.query(
        """
        INSERT INTO FUNCTION s3(s3_full, filename = 'users.json',
                                structure = 'id UInt64, name String, score Float64')
        VALUES (1, 'Alice', 95.5),
               (2, 'Bob', 87.3),
               (3, 'Charlie', 92.1)
        """)

    result = node.query(
        """
        SELECT count(*)
        FROM s3Cluster('cluster_simple', s3_full,
                       filename = 'users.json', structure = 'id UInt64, name String, score Float64')
        """).strip()
    assert result == "3"

    result = node.query(
        """
        SELECT name
        FROM s3Cluster('cluster_simple', s3_full,
                       filename = 'users.json', structure = 'id UInt64, name String, score Float64')
        WHERE score > 90
        ORDER BY name
        """).strip()
    assert "Alice" in result and "Charlie" in result and "Bob" not in result


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
                        SELECT sum(x)
                        FROM s3Cluster('cluster_simple', s3_glob,
                                       filename = 'part_*.csv', format = 'CSV', structure = 'x UInt32')
                        """).strip()
    assert result == "99"


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


def test_s3_with_compression(cluster):
    node = cluster.instances["s3_node"]

    node.query(f"""
        CREATE NAMED COLLECTION s3_gzip AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/compressed/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'CSV',
        compression = 'gzip'
    """)

    node.query(
        "INSERT INTO FUNCTION s3(s3_gzip, filename='data.csv.gz', structure='val UInt64') SELECT number FROM numbers(1000)")

    result = node.query("SELECT sum(val) FROM s3(s3_gzip, filename='data.csv.gz', structure='val UInt64')").strip()
    assert result == "499500"


def test_s3_with_schema_in_collection(cluster):
    node = cluster.instances["s3_node"]

    node.query(f"""
        CREATE NAMED COLLECTION s3_schema AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/schema_test/',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'TabSeparated',
        structure = 'id UInt64, name String, score Float32'
    """)

    node.query(
        "INSERT INTO FUNCTION s3(s3_schema, filename='scores.tsv') VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.3), (3, 'Charlie', 92.1)")

    result = float(node.query("SELECT avg(score) FROM s3(s3_schema, filename='scores.tsv')").strip())
    assert abs(result - 91.63) < 0.1


def test_shared_credentials_multiple_tables(cluster):
    node = cluster.instances["s3_node"]

    node.query(f"""
        CREATE NAMED COLLECTION shared_s3 AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{cluster.minio_bucket}/shared/',
        access_key_id = 'minio' NOT OVERRIDABLE,
        secret_access_key = '{minio_secret_key}' NOT OVERRIDABLE
    """)

    # create multiple tables using same credentials
    for name in ["users", "orders", "products"]:
        node.query(f"DROP TABLE IF EXISTS {name}_s3")
        node.query(f"""
            CREATE TABLE {name}_s3 (id UInt32, data String)
            ENGINE = S3(shared_s3, filename='{name}.csv', format='CSV')
        """)
        node.query(f"INSERT INTO {name}_s3 VALUES (1, '{name}_data')")

    assert node.query("SELECT data FROM users_s3").strip() == "users_data"
    assert node.query("SELECT data FROM orders_s3").strip() == "orders_data"
    assert node.query("SELECT data FROM products_s3").strip() == "products_data"

    for name in ["users", "orders", "products"]:
        node.query(f"DROP TABLE {name}_s3")


@pytest.mark.skip(reason="""https://github.com/ClickHouse/ClickHouse/issues/77366""")
def test_server_starts_with_dropped_collection_table(cluster):
    from helpers.client import QueryRuntimeException

    node = cluster.instances["s3_node"]
    bucket = cluster.minio_bucket

    node.query("DROP TABLE IF EXISTS orphan_table")

    node.query(f"""
        CREATE NAMED COLLECTION orphan_coll AS
        url = 'http://{cluster.minio_host}:{MINIO_INTERNAL_PORT}/{bucket}/orphan_data.csv',
        access_key_id = 'minio',
        secret_access_key = '{minio_secret_key}',
        format = 'CSV'
    """)

    node.query(
        """
        CREATE TABLE orphan_table
        (
            x Int32,
            y Int32
        )
            ENGINE = S3(orphan_coll)
        """)

    node.query(
        """
        INSERT INTO FUNCTION s3(orphan_coll, structure = 'x Int32, y Int32')
        VALUES (1, 10),
               (2, 20)
        """)
    assert node.query("SELECT sum(y) FROM orphan_table").strip() == "30"

    node.query("DROP NAMED COLLECTION orphan_coll")
    assert "orphan_coll" not in node.query("SELECT name FROM system.named_collections")

    node.restart_clickhouse()

    assert node.query("SELECT 1").strip() == "1"

    try:
        node.query("SELECT * FROM orphan_table")
    except QueryRuntimeException as e:
        assert "orphan_coll" in str(e) or "NAMED_COLLECTION_DOESNT_EXIST" in str(e)

    node.query("DROP TABLE IF EXISTS orphan_table")


def test_url_function_with_named_collection(cluster):
    node = cluster.instances["s3_node"]

    node.query("""
        CREATE NAMED COLLECTION url_write_nc AS
        url = 'http://nginx:80/url_nc_test.tsv',
        format = 'TSV',
        structure = 'id UInt32, name String, value Float32',
        method = 'PUT'
    """)

    node.query(
        """
        INSERT INTO FUNCTION url(url_write_nc)
        VALUES (1, 'alice', 10.5),
               (2, 'bob', 20.3),
               (3, 'charlie', 30.7)
        """)

    node.query("""
        CREATE NAMED COLLECTION url_read_nc AS
        url = 'http://nginx:80/url_nc_test.tsv',
        format = 'TSV',
        structure = 'id UInt32, name String, value Float32'
    """)

    result = node.query("SELECT count() FROM url(url_read_nc)").strip()
    assert result == "3"


def test_url_table_engine_with_named_collection(cluster):
    node = cluster.instances["s3_node"]

    node.query("DROP TABLE IF EXISTS url_table_nc")

    node.query("""
        CREATE NAMED COLLECTION url_engine_write_nc AS
        url = 'http://nginx:80/url_engine_test.csv',
        format = 'CSV',
        structure = 'x Int32, y Int32, z Int32',
        method = 'PUT'
    """)

    node.query(
        """
        INSERT INTO FUNCTION url(url_engine_write_nc)
        VALUES (10, 20, 30),
               (40, 50, 60),
               (70, 80, 90)
        """)

    node.query("""
        CREATE NAMED COLLECTION url_engine_read_nc AS
        url = 'http://nginx:80/url_engine_test.csv',
        format = 'CSV'
    """)

    node.query(
        """
        CREATE TABLE url_table_nc
        (
            x Int32,
            y Int32,
            z Int32
        )
            ENGINE = URL(url_engine_read_nc)
        """)

    result = node.query("SELECT count() FROM url_table_nc").strip()
    assert result == "3"

    show_create = node.query("SHOW CREATE TABLE url_table_nc")
    assert "url_engine_read_nc" in show_create

    node.query("DROP TABLE url_table_nc")


def test_url_with_overridable_params(cluster):
    node = cluster.instances["s3_node"]

    node.query("""
        CREATE NAMED COLLECTION url_override_write_nc AS
        url = 'http://nginx:80/url_override_1.json' OVERRIDABLE,
        format = 'JSONEachRow',
        structure = 'a UInt32, b String',
        method = 'PUT'
    """)

    node.query("INSERT INTO FUNCTION url(url_override_write_nc) VALUES (100, 'first')")
    node.query(
        "INSERT INTO FUNCTION url(url_override_write_nc, url='http://nginx:80/url_override_2.json') VALUES (200, 'second')")

    node.query("""
        CREATE NAMED COLLECTION url_override_read_nc AS
        url = 'http://nginx:80/url_override_1.json' OVERRIDABLE,
        format = 'JSONEachRow',
        structure = 'a UInt32, b String'
    """)

    result = node.query("SELECT b FROM url(url_override_read_nc)").strip()
    assert result == "first"

    result = node.query("SELECT b FROM url(url_override_read_nc, url='http://nginx:80/url_override_2.json')").strip()
    assert result == "second"


def test_url_cluster_with_named_collection(cluster):
    node = cluster.instances["s3_node"]

    node.query("""
        CREATE NAMED COLLECTION url_cluster_write_nc AS
        url = 'http://nginx:80/url_cluster_test.csv',
        format = 'CSV',
        structure = 'key String, val UInt32',
        method = 'PUT'
    """)

    node.query("INSERT INTO FUNCTION url(url_cluster_write_nc) VALUES ('alpha', 1), ('beta', 2), ('gamma', 3)")

    node.query("""
        CREATE NAMED COLLECTION url_cluster_read_nc AS
        url = 'http://nginx:80/url_cluster_test.csv',
        format = 'CSV',
        structure = 'key String, val UInt32'
    """)

    result = node.query("SELECT sum(val) FROM urlCluster('cluster_simple', url_cluster_read_nc)").strip()
    assert result == "6"
