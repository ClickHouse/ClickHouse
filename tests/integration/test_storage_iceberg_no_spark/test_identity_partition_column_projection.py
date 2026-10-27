#!/usr/bin/env python3

import io
import uuid
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, LongType, StringType, StructType, TimestampType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)

BASE_URL = "http://rest:8181/v1"

CATALOG_NAME = "demo"

SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="region", field_type=StringType(), required=False),
    NestedField(field_id=3, name="val", field_type=StringType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("region", pa.string(), nullable=True),
        pa.field("val", pa.string(), nullable=True),
    ]
)


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


def create_clickhouse_iceberg_database(node, name):
    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS catalog_type='rest', warehouse='demo', storage_endpoint='http://minio1:9001/warehouse-rest'
    """
    )


def data_file_paths(table):
    return sorted(task.file.file_path for task in table.scan().plan_files())


def drop_column_from_data_file(started_cluster, s3_uri, column):
    """Rewrite the data file at `s3_uri` in place, without `column`."""
    assert s3_uri.startswith("s3://"), s3_uri
    bucket, key = s3_uri[len("s3://") :].split("/", 1)

    response = started_cluster.minio_client.get_object(bucket, key)
    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()

    parquet_table = pq.read_table(pa.BufferReader(content))
    assert column in parquet_table.column_names, s3_uri
    parquet_table = parquet_table.select(
        [name for name in parquet_table.column_names if name != column]
    )

    buffer = io.BytesIO()
    pq.write_table(parquet_table, buffer)
    payload = buffer.getvalue()
    started_cluster.minio_client.put_object(
        bucket, key, io.BytesIO(payload), len(payload)
    )


def test_identity_partition_column_not_stored_in_data_files(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_part",
        schema=SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=2,
                field_id=1000,
                transform=IdentityTransform(),
                name="region",
            )
        ),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "region": "East", "val": "a"},
                {"id": 2, "region": "West", "val": "b"},
                {"id": 3, "region": "East", "val": "c"},
                {"id": 4, "region": "North", "val": "d"},
            ],
            schema=ARROW_SCHEMA,
        )
    )

    files = data_file_paths(table)
    assert len(files) == 3
    for path in files:
        drop_column_from_data_file(started_cluster_iceberg_no_spark, path, "region")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_part`"

    assert instance.query(f"SELECT count() FROM {table_expression}").strip() == "4"

    assert (
        instance.query(
            f"SELECT id, region, val FROM {table_expression} ORDER BY id"
        ).strip()
        == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc\n4\tNorth\td"
    )

    for settings in ["optimize_move_to_prewhere = 1", "optimize_move_to_prewhere = 0"]:
        assert (
            instance.query(
                f"SELECT count() FROM {table_expression} WHERE region = 'East' SETTINGS {settings}"
            ).strip()
            == "2"
        )

    assert (
        instance.query(
            f"SELECT id FROM {table_expression} PREWHERE region = 'East' ORDER BY id"
        ).strip()
        == "1\n3"
    )

    assert (
        instance.query(
            f"SELECT region, count() FROM {table_expression} GROUP BY region ORDER BY region"
        ).strip()
        == "East\t2\nNorth\t1\nWest\t1"
    )

    assert (
        instance.query(
            f"SELECT id, region FROM {table_expression} WHERE val = 'd'"
        ).strip()
        == "4\tNorth"
    )


def test_identity_partition_column_added_by_partition_evolution(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_evolved",
        schema=SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "region": "East", "val": "a"},
                {"id": 2, "region": "West", "val": "b"},
            ],
            schema=ARROW_SCHEMA,
        )
    )
    unpartitioned_files = set(data_file_paths(table))

    with table.update_spec() as update:
        update.add_identity("region")

    table.append(
        pa.Table.from_pylist(
            [
                {"id": 3, "region": "East", "val": "c"},
                {"id": 4, "region": "North", "val": "d"},
            ],
            schema=ARROW_SCHEMA,
        )
    )

    partitioned_files = set(data_file_paths(table)) - unpartitioned_files
    assert len(partitioned_files) == 2
    for path in partitioned_files:
        drop_column_from_data_file(started_cluster_iceberg_no_spark, path, "region")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_evolved`"

    assert (
        instance.query(
            f"SELECT id, region, val FROM {table_expression} ORDER BY id"
        ).strip()
        == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc\n4\tNorth\td"
    )

    assert (
        instance.query(
            f"SELECT count() FROM {table_expression} WHERE region = 'East'"
        ).strip()
        == "2"
    )


def test_identity_partition_column_not_stored_in_data_files_on_cluster(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    bucket = started_cluster_iceberg_no_spark.minio_bucket
    table_name = "test_identity_partition_projection_" + get_uuid_str()

    create_iceberg_table(
        "s3",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int64, region String, val String)",
        format_version=2,
        partition_by="region",
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1,'East','a'),(2,'West','b'),(3,'East','c'),(4,'North','d')",
        settings={"allow_insert_into_iceberg": 1},
    )

    prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    data_files = [
        object.object_name
        for object in started_cluster_iceberg_no_spark.minio_client.list_objects(
            bucket, prefix, recursive=True
        )
        if object.object_name.endswith(".parquet")
    ]
    assert len(data_files) == 3
    for key in data_files:
        drop_column_from_data_file(
            started_cluster_iceberg_no_spark, f"s3://{bucket}/{key}", "region"
        )

    for table_function in [
        get_creation_expression(
            "s3", table_name, started_cluster_iceberg_no_spark, table_function=True
        ),
        get_creation_expression(
            "s3",
            table_name,
            started_cluster_iceberg_no_spark,
            table_function=True,
            run_on_cluster=True,
        ),
    ]:
        assert (
            instance.query(
                f"SELECT id, region, val FROM {table_function} ORDER BY id"
            ).strip()
            == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc\n4\tNorth\td"
        )
        assert (
            instance.query(
                f"SELECT count() FROM {table_function} WHERE region = 'East'"
            ).strip()
            == "2"
        )


TYPED_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="ts", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="d", field_type=DateType(), required=False),
    NestedField(field_id=4, name="n", field_type=LongType(), required=False),
)

TYPED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("ts", pa.timestamp("us"), nullable=True),
        pa.field("d", pa.date32(), nullable=True),
        pa.field("n", pa.int64(), nullable=True),
    ]
)


def test_identity_partition_column_types(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_types",
        schema=TYPED_SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="ts"),
            PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="d"),
            PartitionField(source_id=4, field_id=1002, transform=IdentityTransform(), name="n"),
        ),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "ts": datetime(2024, 5, 17, 10, 20, 30), "d": date(2024, 5, 17), "n": 42},
                {"id": 2, "ts": datetime(2020, 1, 2, 3, 4, 5), "d": date(2020, 1, 2), "n": -7},
            ],
            schema=TYPED_ARROW_SCHEMA,
        )
    )

    files = data_file_paths(table)
    assert len(files) == 2
    for path in files:
        for column in ["ts", "d", "n"]:
            drop_column_from_data_file(started_cluster_iceberg_no_spark, path, column)

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_types`"

    assert (
        instance.query(
            f"SELECT id, toString(ts, 'UTC'), toString(d), n FROM {table_expression} ORDER BY id"
        ).strip()
        == "1\t2024-05-17 10:20:30.000000\t2024-05-17\t42\n2\t2020-01-02 03:04:05.000000\t2020-01-02\t-7"
    )


def test_identity_partition_column_types_written_by_clickhouse(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    bucket = started_cluster_iceberg_no_spark.minio_bucket
    table_name = "test_identity_partition_types_ch_" + get_uuid_str()

    create_iceberg_table(
        "s3",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int64, ts DateTime64(6), d Date32, n Int64)",
        format_version=2,
        partition_by="(ts, d, n)",
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES "
        f"(1, '2024-05-17 10:20:30.000000', '2024-05-17', 42), "
        f"(2, '2020-01-02 03:04:05.000000', '2020-01-02', -7)",
        settings={"allow_insert_into_iceberg": 1},
    )

    prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    data_files = [
        object.object_name
        for object in started_cluster_iceberg_no_spark.minio_client.list_objects(
            bucket, prefix, recursive=True
        )
        if object.object_name.endswith(".parquet")
    ]
    assert len(data_files) == 2
    for key in data_files:
        for column in ["ts", "d", "n"]:
            drop_column_from_data_file(
                started_cluster_iceberg_no_spark, f"s3://{bucket}/{key}", column
            )

    table_function = get_creation_expression(
        "s3", table_name, started_cluster_iceberg_no_spark, table_function=True
    )
    assert (
        instance.query(
            f"SELECT id, toString(ts, 'UTC'), toString(d), n FROM {table_function} ORDER BY id"
        ).strip()
        == "1\t2024-05-17 10:20:30.000000\t2024-05-17\t42\n2\t2020-01-02 03:04:05.000000\t2020-01-02\t-7"
    )


DOTTED_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="a.b", field_type=StringType(), required=False),
    NestedField(field_id=3, name="s", field_type=StructType(
        NestedField(field_id=4, name="c", field_type=StringType(), required=False),
    ), required=False),
)

DOTTED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("a.b", pa.string(), nullable=True),
        pa.field("s", pa.struct([pa.field("c", pa.string(), nullable=True)]), nullable=True),
    ]
)


def test_identity_partition_column_name_with_period(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_dotted",
        schema=DOTTED_SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="a.b"),
            PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="s.c"),
        ),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "a.b": "East", "s": {"c": "x"}},
                {"id": 2, "a.b": "West", "s": {"c": "y"}},
            ],
            schema=DOTTED_ARROW_SCHEMA,
        )
    )

    files = data_file_paths(table)
    assert len(files) == 2
    for path in files:
        drop_column_from_data_file(started_cluster_iceberg_no_spark, path, "a_x2Eb")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_dotted`"

    assert (
        instance.query(f"SELECT id, `a.b`, s.c FROM {table_expression} ORDER BY id").strip()
        == "1\tEast\tx\n2\tWest\ty"
    )


NESTED_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="s", field_type=StructType(
        NestedField(field_id=3, name="c", field_type=StringType(), required=False),
        NestedField(field_id=4, name="d", field_type=LongType(), required=False),
    ), required=False),
)

NESTED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field(
            "s",
            pa.struct(
                [
                    pa.field("c", pa.string(), nullable=True),
                    pa.field("d", pa.int64(), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)


def test_identity_partition_column_nested_field(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_nested",
        schema=NESTED_SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="s.c"),
        ),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "s": {"c": "x", "d": 10}},
                {"id": 2, "s": {"c": "y", "d": 20}},
            ],
            schema=NESTED_ARROW_SCHEMA,
        )
    )

    files = data_file_paths(table)
    assert len(files) == 2
    for path in files:
        drop_column_from_data_file(started_cluster_iceberg_no_spark, path, "s")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_nested`"

    projection = instance.query(
        f"SELECT id, s.c, s.d FROM {table_expression} ORDER BY id"
    ).strip()
    filtered = instance.query(
        f"SELECT id FROM {table_expression} WHERE s.c = 'x' ORDER BY id",
        settings={"optimize_move_to_prewhere": 1},
    ).strip()
    whole_struct = instance.query(
        f"SELECT id, s FROM {table_expression} ORDER BY id"
    ).strip()
    assert (projection, filtered, whole_struct) == (
        "1\tx\t\\N\n2\ty\t\\N",
        "1",
        "1\t(NULL,NULL)\n2\t(NULL,NULL)",
    )



def test_identity_partition_column_with_row_policy(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    bucket = started_cluster_iceberg_no_spark.minio_bucket
    table_name = "test_identity_partition_row_policy_" + get_uuid_str()
    policy_name = "policy_" + get_uuid_str()

    create_iceberg_table(
        "s3",
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(id Int64, region String)",
        format_version=2,
        partition_by="region",
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1, 'East'), (2, 'West'), (3, 'East'), (4, 'North')",
        settings={"allow_insert_into_iceberg": 1},
    )

    prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/"
    for object in started_cluster_iceberg_no_spark.minio_client.list_objects(
        bucket, prefix, recursive=True
    ):
        if object.object_name.endswith(".parquet"):
            drop_column_from_data_file(
                started_cluster_iceberg_no_spark,
                f"s3://{bucket}/{object.object_name}",
                "region",
            )

    instance.query(
        f"CREATE ROW POLICY {policy_name} ON {table_name} USING region = 'East' TO ALL"
    )
    try:
        plan = instance.query(f"EXPLAIN actions=1 SELECT id FROM {table_name}")
        assert "Row-level security filter" not in plan, plan
        assert instance.query(f"SELECT id FROM {table_name} ORDER BY id").strip() == "1\n3"
        assert (
            instance.query(f"SELECT id, region FROM {table_name} ORDER BY id").strip()
            == "1\tEast\n3\tEast"
        )
    finally:
        instance.query(f"DROP ROW POLICY {policy_name} ON {table_name}")


def test_identity_partition_column_renamed_after_write(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    table = catalog.create_table(
        identifier=f"{namespace}.t_renamed",
        schema=SCHEMA,
        location="s3://warehouse-rest/data",
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=2,
                field_id=1000,
                transform=IdentityTransform(),
                name="region",
            )
        ),
    )
    table.append(
        pa.Table.from_pylist(
            [
                {"id": 1, "region": "East", "val": "a"},
                {"id": 2, "region": "West", "val": "b"},
                {"id": 3, "region": "East", "val": "c"},
            ],
            schema=ARROW_SCHEMA,
        )
    )

    for path in data_file_paths(table):
        drop_column_from_data_file(started_cluster_iceberg_no_spark, path, "region")

    with table.update_schema() as update:
        update.rename_column("region", "area")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_renamed`"

    assert (
        instance.query(
            f"SELECT id, area, val FROM {table_expression} ORDER BY id"
        ).strip()
        == "1\tEast\ta\n2\tWest\tb\n3\tEast\tc"
    )
    for move_to_prewhere in [0, 1]:
        assert (
            instance.query(
                f"SELECT id FROM {table_expression} WHERE area = 'East' ORDER BY id",
                settings={"optimize_move_to_prewhere": move_to_prewhere},
            ).strip()
            == "1\n3"
        ), move_to_prewhere
    assert (
        instance.query(
            f"SELECT id FROM {table_expression} PREWHERE area = 'East' ORDER BY id"
        ).strip()
        == "1\n3"
    )
