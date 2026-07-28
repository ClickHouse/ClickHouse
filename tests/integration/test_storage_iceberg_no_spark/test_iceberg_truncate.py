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
    """The truncate snapshot summary must report `operation=delete`, the `deleted-*`
    counters, and zeroed table-wide `total-*` totals, while the preceding ClickHouse
    append must still carry `changed-partition-count`. Encoding truncate as `delete`
    (rather than `overwrite`) is what lets `OPTIMIZE TABLE` data compaction accept a
    post-truncate history -- see `test_iceberg_truncate_then_optimize`. Uses a local (non-catalog) table
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

    # The truncate snapshot: delete, all data removed, running totals reset to 0.
    assert op_truncate == "DELETE", op_truncate
    assert s_truncate["deleted-records"] == "3", s_truncate
    assert int(s_truncate["deleted-data-files"]) >= 1, s_truncate
    assert s_truncate["total-records"] == "0", s_truncate
    assert s_truncate["total-data-files"] == "0", s_truncate
    assert s_truncate["total-delete-files"] == "0", s_truncate
    assert s_truncate["total-position-deletes"] == "0", s_truncate
    assert s_truncate["total-equality-deletes"] == "0", s_truncate


def test_iceberg_truncate_then_optimize(started_cluster_iceberg_no_spark):
    """`OPTIMIZE TABLE` must succeed on a table that has a `TRUNCATE` in its history.

    Data compaction validates the whole snapshot history up front, so a truncate snapshot
    must be a shape it accepts. This drives insert -> truncate -> insert -> `OPTIMIZE TABLE`
    and asserts compaction succeeds and the post-truncate data is intact.

    Note: must be plain `OPTIMIZE TABLE`, not `OPTIMIZE TABLE ... MANIFEST` -- only the
    data-compaction path scans the whole history, so a `MANIFEST` variant would pass
    vacuously. Uses a local (non-catalog) table so ClickHouse both writes and compacts."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_truncate_then_optimize_" + get_uuid_str()

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
    # Reuse the table after truncate so history is APPEND -> DELETE -> APPEND and there is
    # live data for the compactor to consider.
    instance.query(f"INSERT INTO {table_name} VALUES (4), (5), (6);")

    # Guard the premise: the truncate really produced the delete snapshot the fix targets.
    history = read_iceberg_history(instance, "default", table_name)
    assert [op for op, _ in history] == ["APPEND", "DELETE", "APPEND"], history

    # Pre-fix this threw NOT_IMPLEMENTED ("Unsupported snapshot's operation type"); it must now succeed.
    instance.query(
        f"OPTIMIZE TABLE {table_name}",
        settings={"allow_experimental_iceberg_compaction": 1},
    )

    assert int(instance.query(f"SELECT count() FROM {table_name}").strip()) == 3
    assert instance.query(f"SELECT x FROM {table_name} ORDER BY x").split() == ["4", "5", "6"]


def test_iceberg_truncate_rejected_when_pinned_to_explicit_metadata_file(started_cluster_iceberg_no_spark):
    """A direct-path (catalog-less) Iceberg table pinned to a specific metadata file via the
    `iceberg_metadata_file_path` setting freezes its reads at that snapshot — the setting is a
    time-travel / reproducible-read feature. A `TRUNCATE` would write a fresh snapshot that the
    table's own reads never observe (they keep resolving the pinned file), so it must be rejected
    with `BAD_ARGUMENTS` rather than silently committing an invisible truncate. Catalog-backed
    tables are intentionally exempt: the catalog re-resolves the effective metadata location on
    every access, so the pin is refreshed rather than stale."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    write_settings = {"allow_insert_into_iceberg": 1}

    suffix = get_uuid_str()
    # Both ClickHouse tables below point at the SAME Iceberg table directory on local disk.
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/iceberg_truncate_pinned_{suffix}"
    engine = f"IcebergLocal(local, path = '{table_path}', format = Parquet)"

    live_table = f"iceberg_truncate_live_{suffix}"
    pinned_table = f"iceberg_truncate_pinned_{suffix}"

    # 1. Create + populate the live table. For a catalog-less table ClickHouse writes metadata
    #    files with deterministic, non-UUID names: CREATE writes `metadata/v1.metadata.json` (the
    #    empty snapshot) and this INSERT writes `metadata/v2.metadata.json` (three rows).
    instance.query(
        f"CREATE TABLE {live_table} (x Int) ENGINE = {engine}",
        settings=write_settings,
    )
    instance.query(f"INSERT INTO {live_table} VALUES (1), (2), (3)", settings=write_settings)
    assert instance.query(f"SELECT count() FROM {live_table}").strip() == "3"

    # 2. Attach a SECOND table over the same directory, pinned to the now-stale empty v1 snapshot.
    #    `IF NOT EXISTS` makes the engine attach to the existing metadata instead of trying to
    #    re-initialize `v1` (`createInitial` sees the existing metadata files and would otherwise
    #    raise `TABLE_ALREADY_EXISTS`).
    instance.query(
        f"""
        CREATE TABLE IF NOT EXISTS {pinned_table} (x Int) ENGINE = {engine}
        SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json'
        """,
        settings=write_settings,
    )
    # The pin genuinely freezes reads at the empty v1 snapshot — precisely why a truncate here
    # would be invisible to readers of this table.
    assert instance.query(f"SELECT count() FROM {pinned_table}").strip() == "0"

    # 3. TRUNCATE on the pinned table must be rejected up front, and the error must name the
    #    offending setting so the user knows the remedy.
    error = instance.query_and_get_error(f"TRUNCATE TABLE {pinned_table}", settings=write_settings)
    assert "BAD_ARGUMENTS" in error, error
    assert "iceberg_metadata_file_path" in error, error

    # 4. The same operation on the unpinned (auto-discovering) table over the same data is still
    #    allowed — the guard is scoped to pinned tables only, not to local Iceberg tables at large.
    instance.query(f"TRUNCATE TABLE {live_table}", settings=write_settings)
    assert instance.query(f"SELECT count() FROM {live_table}").strip() == "0"
