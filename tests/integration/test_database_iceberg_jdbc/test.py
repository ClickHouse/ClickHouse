import uuid

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType
from sqlalchemy.engine import URL

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key, pg_pass
from helpers.postgres_utility import get_postgres_conn


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/hosts.xml"], with_postgres=True, with_minio=True
)
BUCKET = "jdbc-catalog"
READER_PASSWORD = "jdbc_reader_password"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        cluster.minio_client.make_bucket(BUCKET)
        with get_postgres_conn(cluster.postgres_ip, cluster.postgres_port) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE ROLE jdbc_reader LOGIN PASSWORD '{READER_PASSWORD}'")
        yield cluster
    finally:
        cluster.shutdown()


def database_sql(name, database, endpoint, host="postgres1", port=5432):
    return f"""
        CREATE DATABASE {name} ENGINE = DataLakeCatalog
        SETTINGS catalog_type='jdbc', warehouse='jdbc_test',
            jdbc_host='{host}', jdbc_port={port}, jdbc_database='{database}',
            jdbc_user='jdbc_reader', jdbc_password='{READER_PASSWORD}',
            vended_credentials=0, storage_endpoint='{endpoint}',
            aws_access_key_id='{minio_access_key}', aws_secret_access_key='{minio_secret_key}'
    """


@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("endpoint_has_bucket", [False, True])
def test_catalog_reads_and_refresh(started_cluster, version, endpoint_has_bucket):
    name = "jdbc_" + uuid.uuid4().hex
    admin = get_postgres_conn(cluster.postgres_ip, cluster.postgres_port)
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE {name}")
    conn = get_postgres_conn(
        cluster.postgres_ip, cluster.postgres_port, database=True, database_name=name
    )
    uri = URL.create(
        "postgresql+psycopg2", username="postgres", password=pg_pass,
        host=cluster.postgres_ip, port=cluster.postgres_port, database=name,
    ).render_as_string(hide_password=False)
    catalog = SqlCatalog(
        "jdbc_test", uri=uri, warehouse=f"s3://{BUCKET}/{name}",
        **{
            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )
    catalog.create_namespace(("a", "b"))
    catalog.create_namespace("empty")
    table = catalog.create_table(
        ("a", "b", "t"), Schema(NestedField(1, "id", LongType(), required=False))
    )
    table.append(pa.table({"id": pa.array([1, 2], type=pa.int64())}))
    with conn.cursor() as cursor:
        if version == 1:
            cursor.execute("ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5)")
            cursor.execute(
                "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, iceberg_type) "
                "VALUES ('jdbc_test', 'a.b', 'view', 's3://nonexistent/view.metadata.json', 'VIEW')"
            )
        cursor.execute(
            "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location) "
            "VALUES ('another_catalog', 'a.b', 'hidden', 's3://nonexistent/hidden.metadata.json')"
        )
        cursor.execute("GRANT USAGE ON SCHEMA public TO jdbc_reader")
        cursor.execute("GRANT SELECT ON iceberg_tables, iceberg_namespace_properties TO jdbc_reader")

    endpoint = "http://minio1:9001" + (f"/{BUCKET}" if endpoint_has_bucket else "")
    try:
        node.query(database_sql(name, name, endpoint), settings={"allow_database_iceberg": 1})
        assert node.query(f"SHOW TABLES FROM {name}") == "a.b.t\n"
        assert node.query(f"EXISTS TABLE {name}.`a.b.missing`") == "0\n"
        for _ in range(2):
            assert node.query(f"SELECT id FROM {name}.`a.b.t` ORDER BY id") == "1\n2\n"

        # A new metadata pointer must be observed even with the old JSON cached.
        table.append(pa.table({"id": pa.array([3], type=pa.int64())}))
        assert node.query(f"SELECT id FROM {name}.`a.b.t` ORDER BY id") == "1\n2\n3\n"
        with table.update_schema() as update:
            update.add_column("label", StringType())
        assert "label" in node.query(f"DESCRIBE TABLE {name}.`a.b.t`")
        definition = node.query(f"SHOW CREATE DATABASE {name}")
        assert READER_PASSWORD not in definition
        assert "jdbc_password = '[HIDDEN]'" in definition
    finally:
        node.query(f"DROP DATABASE IF EXISTS {name}")
        catalog.engine.dispose()
        conn.close()
        with admin.cursor() as cursor:
            cursor.execute(f"DROP DATABASE {name}")
        admin.close()


def test_outbound_host_policy(started_cluster):
    error = node.query_and_get_error(
        database_sql("jdbc_blocked", "postgres", "http://minio1:9001", host="blocked.invalid"),
        settings={"allow_database_iceberg": 1},
    )
    assert "UNACCEPTABLE_URL" in error


@pytest.mark.parametrize("port", [0, 65536])
def test_invalid_port(started_cluster, port):
    error = node.query_and_get_error(
        database_sql("jdbc_bad_port", "postgres", "http://minio1:9001", port=port),
        settings={"allow_database_iceberg": 1},
    )
    assert "jdbc_port" in error and "1-65535" in error
