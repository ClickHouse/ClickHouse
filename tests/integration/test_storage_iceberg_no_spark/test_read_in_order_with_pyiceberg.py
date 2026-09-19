#!/usr/bin/env python3

from pyiceberg.catalog import load_catalog
from helpers.config_cluster import minio_secret_key, minio_access_key
import uuid
import pyarrow as pa
from datetime import date, timedelta
from pyiceberg.schema import Schema, NestedField
import random
from pyiceberg.types import (
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform, TruncateTransform

BASE_URL = "http://rest:8181/v1"

CATALOG_NAME = "demo"

def load_catalog_impl(started_cluster):
    base_url_local_raw = f"http://localhost:{started_cluster.iceberg_rest_catalog_port}"
    return load_catalog(
        CATALOG_NAME,
        **{
            "uri": base_url_local_raw,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )

def create_table(
    catalog,
    namespace,
    table,
    schema,
    partition_spec,
    sort_order
):
    return catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location="s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )

def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio1:9001/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result


def test_sort_order(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(
            field_id=1, name="boolean_col", field_type=BooleanType(), required=False
        ),
        NestedField(field_id=2, name="long_col", field_type=LongType(), required=False),
        NestedField(
            field_id=3, name="double_col", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=4, name="string_col", field_type=StringType(), required=False
        ),
        NestedField(field_id=5, name="date_col", field_type=DateType(), required=False),
    )

    partition_spec = PartitionSpec()

    # NOTE pyiceberg ignores sort order when writing data, so writes data unsorted
    sort_order = SortOrder(SortField(source_id=4, transform=IdentityTransform()))
    table = create_table(catalog, root_namespace, "test", schema, partition_spec, sort_order)

    data = []
    for _ in range(100):
        data.append(
            {
                "boolean_col": random.choice([True, False]),
                "long_col": random.randint(1000, 10000),
                "double_col": round(random.uniform(1.0, 500.0), 2),
                "string_col": f"User{random.randint(1, 1000)}",
                "date_col": date.today()
                - timedelta(days=random.randint(0, 3650)),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster_iceberg_no_spark, instance, CATALOG_NAME)
    print(instance.query(f"SHOW TABLES FROM {CATALOG_NAME};"))

    # NOTE Read in order optimization shouldn't work because data is not sorted
    result = instance.query(f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=0").strip().split("\n")
    assert result == list(sorted(result))
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=0"
        )
    )

    result = instance.query(f"SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=1").strip().split("\n")
    assert 'PartialSortingTransform' in (
        instance.query(
            f"EXPLAIN PIPELINE SELECT string_col FROM {CATALOG_NAME}.`{root_namespace}.test` ORDER BY string_col SETTINGS optimize_read_in_order=1"
        )
    )

    assert result == list(sorted(result))


def test_sort_order_special_char_column_name(started_cluster_iceberg_no_spark):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/110123
    # An Iceberg table whose default sort order references a column that needs
    # quoting (e.g. `@timestamp`) used to be unreadable: the synthesized storage
    # ORDER BY was built from the raw column name and failed to parse with
    # SYNTAX_ERROR. The column name must be backquoted.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(
            field_id=1, name="@timestamp", field_type=StringType(), required=False
        ),
        NestedField(field_id=2, name="long_col", field_type=LongType(), required=False),
    )

    partition_spec = PartitionSpec()
    sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))
    table = create_table(
        catalog, root_namespace, "test_special", schema, partition_spec, sort_order
    )

    data = []
    for _ in range(100):
        data.append(
            {
                "@timestamp": f"ts{random.randint(1, 1000)}",
                "long_col": random.randint(1000, 10000),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )

    # Before the fix this failed with Code 62 (SYNTAX_ERROR) on any read.
    assert (
        instance.query(
            f"SELECT count() FROM {CATALOG_NAME}.`{root_namespace}.test_special`"
        ).strip()
        == "100"
    )

    result = (
        instance.query(
            f"SELECT `@timestamp` FROM {CATALOG_NAME}.`{root_namespace}.test_special` ORDER BY `@timestamp` SETTINGS optimize_read_in_order=1"
        )
        .strip()
        .split("\n")
    )
    assert result == list(sorted(result))


def test_sort_order_transform_special_char_column_name(started_cluster_iceberg_no_spark):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/110123
    # Covers the transformed branch of getSortingKeyDescriptionFromMetadata: the
    # synthesized ORDER BY wraps the column in a transform (e.g. icebergTruncate),
    # so the quoted name must be produced inside the transform call too. Without
    # the backquote the clause `icebergTruncate(4, @timestamp) ASC` fails to parse
    # with SYNTAX_ERROR.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(
            field_id=1, name="@timestamp", field_type=StringType(), required=False
        ),
        NestedField(field_id=2, name="long_col", field_type=LongType(), required=False),
    )

    partition_spec = PartitionSpec()
    sort_order = SortOrder(
        SortField(source_id=1, transform=TruncateTransform(width=4))
    )
    table = create_table(
        catalog, root_namespace, "test_special_transform", schema, partition_spec, sort_order
    )

    data = []
    for _ in range(100):
        data.append(
            {
                "@timestamp": f"ts{random.randint(1, 1000)}",
                "long_col": random.randint(1000, 10000),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )

    # Before the fix this failed with Code 62 (SYNTAX_ERROR) on any read because
    # the transformed sort key was built as icebergTruncate(4, @timestamp).
    assert (
        instance.query(
            f"SELECT count() FROM {CATALOG_NAME}.`{root_namespace}.test_special_transform`"
        ).strip()
        == "100"
    )


def test_sort_order_through_merge_table(started_cluster_iceberg_no_spark):
    # Regression test for reading an Iceberg table through a `Merge` table.
    # An Iceberg table is only sorted by its sorting key when every data file
    # carries the table's sort order id - and even then the object storage
    # pipeline does not preserve file order yet
    # (https://github.com/ClickHouse/ClickHouse/issues/112981) - so the object
    # storage arm of `recursivelyApplyToReadingSteps` fails closed and rejects
    # reading in order through a `Merge` table. Here `pyiceberg` declares a sort
    # order but writes the data unsorted, making the danger concrete:
    # `ReadFromMerge::requestReadingInOrder` used to ignore object storage
    # children and advertise the order of the declared sorting key on its own,
    # which dropped the sorting step and returned unsorted rows.
    #
    # Note that the request can also be rejected earlier, because a `Merge` table
    # does not refresh the metadata of the tables it selects, so their sorting key
    # is not always visible to `checkSupportedReadingStep`. This check is
    # fail-closed either way: the rows must come out sorted.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(
            field_id=1, name="string_col", field_type=StringType(), required=False
        ),
        NestedField(field_id=2, name="long_col", field_type=LongType(), required=False),
    )

    partition_spec = PartitionSpec()
    # NOTE pyiceberg ignores the sort order when writing data, so the data is unsorted.
    sort_order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))
    table = create_table(
        catalog, root_namespace, "test_merge", schema, partition_spec, sort_order
    )

    data = []
    for _ in range(100):
        data.append(
            {
                "string_col": f"User{random.randint(1, 1000)}",
                "long_col": random.randint(1000, 10000),
            }
        )

    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )

    merge_source = f"merge('{CATALOG_NAME}', '^{root_namespace}\\\\.test_merge$')"

    assert (
        instance.query(f"SELECT count() FROM {merge_source}").strip() == "100"
    )

    # The order request must be rejected for the object storage child, so the
    # sorting step stays in the pipeline and the result is sorted by it.
    assert "PartialSortingTransform" in instance.query(
        f"EXPLAIN PIPELINE SELECT string_col FROM {merge_source} "
        f"ORDER BY string_col SETTINGS optimize_read_in_order = 1"
    )

    result = (
        instance.query(
            f"SELECT string_col FROM {merge_source} "
            f"ORDER BY string_col SETTINGS optimize_read_in_order = 1"
        )
        .strip()
        .split("\n")
    )
    assert result == list(sorted(result))


def test_top_k_through_join_does_not_defer_for_merge_object_storage(started_cluster_iceberg_no_spark):
    # `topKThroughJoin` must retain its `Sort + Limit` pushdown when the preserved
    # input is a `Merge` table with an object-storage child. The actual
    # `ReadFromMerge::requestReadingInOrder` rejects that child, so deferring to
    # the read-in-order pass would otherwise leave the query with neither plan.
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    root_namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    schema = Schema(
        NestedField(field_id=1, name="key", field_type=LongType(), required=False),
        NestedField(field_id=2, name="payload", field_type=StringType(), required=False),
    )
    table = create_table(
        catalog,
        root_namespace,
        "test_top_k_merge",
        schema,
        PartitionSpec(),
        SortOrder(SortField(source_id=1, transform=IdentityTransform())),
    )
    table.append(pa.Table.from_pylist([{"key": key, "payload": str(key)} for key in range(100)]))

    create_clickhouse_iceberg_database(
        started_cluster_iceberg_no_spark, instance, CATALOG_NAME
    )
    merge_source = f"merge('{CATALOG_NAME}', '^{root_namespace}\\\\.test_top_k_merge$')"

    instance.query("DROP TABLE IF EXISTS top_k_merge_object_storage_right")
    instance.query(
        "CREATE TABLE top_k_merge_object_storage_right (key Int64, value String) "
        "ENGINE = MergeTree ORDER BY key"
    )
    instance.query("INSERT INTO top_k_merge_object_storage_right VALUES (0, 'zero')")

    plan = instance.query(
        f"EXPLAIN actions = 0 SELECT left.key, right.value FROM {merge_source} AS left "
        "LEFT JOIN top_k_merge_object_storage_right AS right ON right.key = left.key "
        "ORDER BY left.key LIMIT 3 "
        "SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, "
        "query_plan_read_in_order_through_join = 1, query_plan_top_k_through_join = 1, "
        "query_plan_join_swap_table = 0, query_plan_max_limit_for_top_k_optimization = 0, "
        "enable_parallel_replicas = 0, max_bytes_before_external_join = 0, "
        "max_bytes_ratio_before_external_join = 0"
    )
    assert plan.count("Sorting") >= 2
    assert plan.count("Limit") >= 2

    instance.query("DROP TABLE top_k_merge_object_storage_right")
