#!/usr/bin/env python3

import uuid
from concurrent.futures import ThreadPoolExecutor

from helpers.config_cluster import minio_secret_key, minio_access_key
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.table.sorting import SortOrder
from pyiceberg.transforms import IdentityTransform

BASE_URL = "http://rest:8181/v1"
CATALOG_NAME = "demo"


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


def create_clickhouse_iceberg_database(started_cluster, node, name):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k + "=" + repr(v) for k, v in settings.items()))}
    """
    )


def create_partitioned_catalog_table(catalog, namespace, table_name):
    """Create a REST-catalog table partitioned by identity(a), matching the schema used by the
    catalog-backed DROP PARTITION tests."""
    schema = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="a")
    )
    catalog.create_namespace(namespace)
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location="s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=SortOrder(),
    )


def test_drop_partition_catalog_backed(started_cluster_iceberg_no_spark):
    """DROP PARTITION on a catalog-backed (REST) table must commit the new metadata
    location to the catalog, so that an independent catalog reader (pyiceberg) also
    sees the dropped partition. Before the fix the executor only wrote the metadata
    file and never called catalog->updateMetadata, leaving the catalog stale."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    namespace = f"clickhouse_{uuid.uuid4()}"
    table_name = "drop_partition"

    schema = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="a")
    )
    catalog.create_namespace(namespace)
    catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
        location="s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=SortOrder(),
    )

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    qualified = f"{CATALOG_NAME}.`{namespace}.{table_name}`"

    instance.query(
        f"INSERT INTO {qualified} VALUES (1, 'x'), (2, 'y'), (3, 'z')",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {qualified}").strip() == "3"

    instance.query(
        f"ALTER TABLE {qualified} DROP PARTITION 2",
        settings={"allow_insert_into_iceberg": 1},
    )

    # ClickHouse sees the drop.
    assert instance.query(f"SELECT a FROM {qualified} ORDER BY a").strip() == "1\n3"

    # Re-create the database so the table's metadata location is re-read from the catalog. Before the
    # fix the DROP only wrote the metadata file and never called catalog->updateMetadata, so a fresh
    # catalog reader would still see the pre-drop metadata (3 rows).
    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    assert instance.query(f"SELECT count() FROM {qualified}").strip() == "2"
    assert instance.query(f"SELECT a FROM {qualified} ORDER BY a").strip() == "1\n3"


def test_drop_partition_catalog_concurrent_insert_survives(started_cluster_iceberg_no_spark):
    """DROP PARTITION on a catalog-backed (REST) table should still do the right thing when another
    query changes the table at the same time.

    The scenario: we start dropping partition a=1 and pause it the moment it has decided which files
    to remove. While it is paused, a second query inserts a new row into that same partition and
    commits. We then let the DROP continue. Its first attempt to commit is refused by the catalog
    because the table moved on underneath it, so it retries from the table's new state and succeeds.

    Afterwards we check that the outcome is correct and consistent: the rows that were in a=1 before
    the drop are gone, the row inserted during the drop is kept, the untouched partition a=2 is
    still there, and re-reading the table fresh from the catalog gives the same answer."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    namespace = f"clickhouse_{uuid.uuid4()}"
    table_name = "drop_partition_concurrent"

    create_partitioned_catalog_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    qualified = f"{CATALOG_NAME}.`{namespace}.{table_name}`"

    instance.query(
        f"INSERT INTO {qualified} VALUES (1, 'before-drop-1'), (1, 'before-drop-2'), (2, 'keep')",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert instance.query(f"SELECT count() FROM {qualified}").strip() == "3"

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")

    executor = ThreadPoolExecutor(max_workers=3)
    try:
        # 1. Wait for the executor to pause at the failpoint.
        wait_future = executor.submit(
            lambda: instance.query(
                "SYSTEM WAIT FAILPOINT iceberg_drop_partition_pause_after_discovery PAUSE",
                timeout=60,
            )
        )

        # 2. Start the DROP PARTITION; it pauses right after discovering files in partition a=1.
        drop_future = executor.submit(
            lambda: instance.query(
                f"ALTER TABLE {qualified} DROP PARTITION 1",
                settings={"allow_insert_into_iceberg": 1},
                timeout=120,
            )
        )

        # Wait for the pause to actually trigger before doing the concurrent INSERT.
        wait_future.result(timeout=60)

        # 3. Concurrent INSERT into the SAME partition (a=1). It commits a new snapshot to the
        #    catalog (advancing the `main` ref) that the DROP retry must observe.
        instance.query(
            f"INSERT INTO {qualified} VALUES (1, 'inserted-during-drop')",
            settings={"allow_insert_into_iceberg": 1},
        )

        # 4. Release the failpoint. DROP attempt 0's commit fails the catalog's
        #    assert-ref-snapshot-id requirement; the retry re-reads the catalog pointer and commits.
        instance.query("SYSTEM DISABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")

        # 5. DROP completes (must not spin until the 120s timeout).
        drop_future.result(timeout=120)
    finally:
        # Always disable the failpoint -- if anything above raised before the explicit DISABLE ran
        # (e.g. the WAIT timed out), it would otherwise stay enabled and poison subsequent tests.
        try:
            instance.query("SYSTEM DISABLE FAILPOINT iceberg_drop_partition_pause_after_discovery")
        except Exception:
            pass
        executor.shutdown(wait=False)

    # 6. The two pre-drop files for a=1 are gone; the concurrently-inserted row survives, as does
    #    the unrelated partition a=2.
    expected = "1\tinserted-during-drop\n2\tkeep"
    assert instance.query(f"SELECT a, b FROM {qualified} ORDER BY a, b").strip() == expected

    # 7. The catalog itself reflects the same state: re-read the metadata location from the catalog
    #    by re-creating the database, and confirm an independent reader agrees.
    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    assert instance.query(f"SELECT a, b FROM {qualified} ORDER BY a, b").strip() == expected
