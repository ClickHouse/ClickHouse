#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType
from pyiceberg.partitioning import PartitionSpec

CATALOG_NAME = "demo"

WAREHOUSE_BUCKET = "warehouse-rest"

def load_catalog_impl(started_cluster):
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def create_rest_backed_table(started_cluster, name_prefix):
    """Create a fresh REST-catalog namespace + table (via PyIceberg) and a matching
    ClickHouse `DataLakeCatalog` database. Returns everything the tests need to drive
    and inspect the table."""
    instance = started_cluster.instances["node1"]
    catalog = load_catalog_impl(started_cluster)

    namespace = f"{name_prefix}_{uuid.uuid4().hex}"
    catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="val", field_type=StringType(), required=False),
    )
    table_name = "t"
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location=f"s3://{WAREHOUSE_BUCKET}/{namespace}.{table_name}",
        partition_spec=PartitionSpec(),
    )

    ch_table_identifier = f"`{namespace}.{table_name}`"

    instance.query(f"DROP DATABASE IF EXISTS {namespace}")
    instance.query(
        f"""
        CREATE DATABASE {namespace} ENGINE = DataLakeCatalog('http://rest:8181/v1', 'minio', '{minio_secret_key}')
        SETTINGS
            catalog_type='rest',
            warehouse='demo',
            storage_endpoint='http://minio1:9001/{WAREHOUSE_BUCKET}';
        """,
        settings={"allow_database_iceberg": 1},
    )
    return instance, catalog, namespace, table_name, ch_table_identifier


def test_iceberg_truncate_restart(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    namespace = f"clickhouse_truncate_restart_{uuid.uuid4().hex}"
    catalog.create_namespace(namespace)

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="val", field_type=StringType(), required=False),
    )
    table_name = "test_truncate_restart"
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location=f"s3://warehouse-rest/{namespace}.{table_name}",
        partition_spec=PartitionSpec(),
    )

    ch_table_identifier = f"`{namespace}.{table_name}`"

    instance.query(f"DROP DATABASE IF EXISTS {namespace}")
    instance.query(
        f"""
        CREATE DATABASE {namespace} ENGINE = DataLakeCatalog('http://rest:8181/v1', 'minio', '{minio_secret_key}')
        SETTINGS
            catalog_type='rest',
            warehouse='demo',
            storage_endpoint='http://minio1:9001/warehouse-rest';
        """,
        settings={"allow_database_iceberg": 1}
    )

    # 1. Insert initial data and truncate
    df = pa.Table.from_pylist([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}])
    catalog.load_table(f"{namespace}.{table_name}").append(df)

    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 2

    instance.query(
        f"TRUNCATE TABLE {namespace}.{ch_table_identifier}",
        settings={"allow_experimental_insert_into_iceberg": 1}
    )
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 0

    # 2. Restart ClickHouse and verify table is still readable (count = 0)
    instance.restart_clickhouse()
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 0

    # 3. Insert new data after restart and verify it's readable
    new_df = pa.Table.from_pylist([{"id": 3, "val": "C"}])
    catalog.load_table(f"{namespace}.{table_name}").append(new_df)
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch_table_identifier}").strip()) == 1

    instance.query(f"DROP DATABASE {namespace}")


def test_iceberg_truncate_force_latest_under_stale_cache(started_cluster_iceberg_no_spark):
    """TRUNCATE must commit against the freshest catalog state, not a stale cached one.
    With a large staleness window and an external (PyIceberg) commit that ClickHouse's
    cache does not yet reflect, TRUNCATE must still re-read the latest metadata
    (force-latest in its retry loop) and succeed, rather than committing against a stale
    parent snapshot and failing the optimistic-concurrency check."""
    instance, catalog, namespace, table_name, ch = create_rest_backed_table(
        started_cluster_iceberg_no_spark, "clickhouse_truncate_forcelatest"
    )

    catalog.load_table(f"{namespace}.{table_name}").append(
        pa.Table.from_pylist([{"id": 1, "val": "A"}])
    )

    # Prime ClickHouse's cache with this version under a large staleness window.
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {namespace}.{ch} SETTINGS iceberg_metadata_staleness_ms=600000"
            ).strip()
        )
        == 1
    )

    # An external writer advances the table; ClickHouse's cache is now stale.
    catalog.load_table(f"{namespace}.{table_name}").append(
        pa.Table.from_pylist([{"id": 2, "val": "B"}])
    )

    # Even reading with the large staleness window (which would let the old, cache-based
    # read use the stale parent), TRUNCATE must re-read fresh and commit successfully.
    instance.query(
        f"TRUNCATE TABLE {namespace}.{ch}",
        settings={
            "allow_experimental_insert_into_iceberg": 1,
            "iceberg_metadata_staleness_ms": 600000,
        },
    )

    assert (
        int(
            instance.query(
                f"SELECT count() FROM {namespace}.{ch} SETTINGS iceberg_metadata_staleness_ms=600000"
            ).strip()
        )
        == 0
    )

    instance.query(f"DROP DATABASE {namespace}")


def test_iceberg_truncate_no_stray_metadata_file(started_cluster_iceberg_no_spark):
    """On a transactional (REST) catalog the catalog owns the metadata JSON, so a
    TRUNCATE must not also write its own `v<N>.metadata.json`. Otherwise the table
    location ends up with two metadata files for the same version and non-catalog
    (direct-path) readers pick one nondeterministically. Assert TRUNCATE adds exactly
    one new metadata.json (the catalog's), not two."""
    instance, catalog, namespace, table_name, ch = create_rest_backed_table(
        started_cluster_iceberg_no_spark, "clickhouse_truncate_stray"
    )

    catalog.load_table(f"{namespace}.{table_name}").append(
        pa.Table.from_pylist([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}])
    )
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch}").strip()) == 2

    def count_metadata_json():
        prefix = f"{namespace}.{table_name}/metadata/"
        return sum(
            1
            for obj in started_cluster_iceberg_no_spark.minio_client.list_objects(
                WAREHOUSE_BUCKET, prefix=prefix, recursive=True
            )
            if obj.object_name.endswith(".metadata.json")
        )

    before = count_metadata_json()
    instance.query(
        f"TRUNCATE TABLE {namespace}.{ch}",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )
    assert int(instance.query(f"SELECT count() FROM {namespace}.{ch}").strip()) == 0
    after = count_metadata_json()

    assert after == before + 1, (
        "TRUNCATE on a transactional catalog must add exactly one metadata.json "
        f"(the catalog's), but added {after - before} (before={before}, after={after})"
    )

    instance.query(f"DROP DATABASE {namespace}")
