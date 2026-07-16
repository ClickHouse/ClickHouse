#!/usr/bin/env python3

import json
from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.iceberg_utils import create_iceberg_table, get_uuid_str
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
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
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
            storage_endpoint='http://minio:9000/{WAREHOUSE_BUCKET}';
        """,
        settings={"allow_database_iceberg": 1},
    )
    return instance, catalog, namespace, table_name, ch_table_identifier


def read_iceberg_history(instance, database, table_name):
    """Return the ordered [(operation, summary_dict), ...] from system.iceberg_history."""
    rows = (
        instance.query(
            f"SELECT operation, toJSONString(summary) "
            f"FROM system.iceberg_history "
            f"WHERE database = '{database}' AND table = '{table_name}' "
            f"ORDER BY made_current_at FORMAT TSV"
        )
        .strip()
        .split("\n")
    )
    return [(op, json.loads(summary)) for op, summary in (row.split("\t", 1) for row in rows)]


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
            storage_endpoint='http://minio:9000/warehouse-rest';
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


def test_iceberg_truncate_invalidates_metadata_cache(started_cluster_iceberg_no_spark):
    """A successful TRUNCATE must invalidate the local metadata cache, so a subsequent
    read on the SAME server sees the empty table even with a large
    `iceberg_metadata_staleness_ms`. Without `invalidateMetadataCache()` on the success
    path, the pre-truncate LatestMetadataVersion stays cached and the read keeps
    returning stale (non-zero) results until the staleness window expires."""
    instance, catalog, namespace, table_name, ch = create_rest_backed_table(
        started_cluster_iceberg_no_spark, "clickhouse_truncate_cache"
    )

    catalog.load_table(f"{namespace}.{table_name}").append(
        pa.Table.from_pylist([{"id": 1, "val": "A"}, {"id": 2, "val": "B"}])
    )

    # Populate the cache with the pre-truncate version under a large staleness window.
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {namespace}.{ch} SETTINGS iceberg_metadata_staleness_ms=600000"
            ).strip()
        )
        == 2
    )

    instance.query(
        f"TRUNCATE TABLE {namespace}.{ch}",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    # Same server, same large staleness window: the truncate must be observed immediately.
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {namespace}.{ch} SETTINGS iceberg_metadata_staleness_ms=600000"
            ).strip()
        )
        == 0
    )

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


def test_iceberg_truncate_snapshot_summary(started_cluster_iceberg_no_spark):
    """The truncate snapshot summary must report `operation=overwrite`, the `deleted-*`
    counters, and zeroed table-wide `total-*` totals, while the preceding ClickHouse
    append must still carry `changed-partition-count`. Uses a local (non-catalog) table
    so ClickHouse both writes and truncates, reading the summaries back via
    `system.iceberg_history` — which also covers the filesystem truncate path that
    writes the metadata file and version hint directly."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_truncate_summary_" + get_uuid_str()

    create_iceberg_table(
        "local",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(x Int)",
        2,
    )

    instance.query(f"INSERT INTO {table_name} VALUES (1), (2), (3);")
    instance.query(
        f"TRUNCATE TABLE {table_name}",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    history = read_iceberg_history(instance, "default", table_name)
    assert len(history) == 2, f"expected append + truncate snapshots, got: {history}"
    (op_append, s_append), (op_truncate, s_truncate) = history

    # The ClickHouse append must still emit changed-partition-count (restored after the
    # upstream merge dropped it).
    assert op_append == "APPEND", op_append
    assert s_append["changed-partition-count"] == "1", s_append
    assert s_append["total-records"] == "3", s_append

    # The truncate snapshot: overwrite, all data removed, running totals reset to 0.
    assert op_truncate == "OVERWRITE", op_truncate
    assert s_truncate["deleted-records"] == "3", s_truncate
    assert int(s_truncate["deleted-data-files"]) >= 1, s_truncate
    assert s_truncate["total-records"] == "0", s_truncate
    assert s_truncate["total-data-files"] == "0", s_truncate
    assert s_truncate["total-delete-files"] == "0", s_truncate
    assert s_truncate["total-position-deletes"] == "0", s_truncate
    assert s_truncate["total-equality-deletes"] == "0", s_truncate