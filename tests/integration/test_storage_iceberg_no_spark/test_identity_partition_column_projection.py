#!/usr/bin/env python3

import io
import json
import uuid
from datetime import date, datetime

import avro.datafile
import avro.io
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    ListType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

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


def get_s3_object(started_cluster, s3_uri):
    """Read the object at `s3_uri`, returning its bucket, key and bytes."""
    assert s3_uri.startswith("s3://"), s3_uri
    bucket, key = s3_uri[len("s3://") :].split("/", 1)

    response = started_cluster.minio_client.get_object(bucket, key)
    try:
        return bucket, key, response.read()
    finally:
        response.close()
        response.release_conn()


def put_s3_object(started_cluster, bucket, key, payload):
    started_cluster.minio_client.put_object(
        bucket, key, io.BytesIO(payload), len(payload)
    )


def append_field_to_snapshot_schema(started_cluster, table, field):
    """Append `field` to the schema the current snapshot points at, in every copy of that schema.

    A schema-id is immutable per the Iceberg spec, and the reader adds the schema from metadata.json
    as well as from each manifest file, so a copy left unpatched is malformed metadata rather than a
    partial edit: `addIcebergTableSchema` rejects it with `ICEBERG_SPECIFICATION_VIOLATION`.
    """
    bucket, key, content = get_s3_object(started_cluster, table.metadata_location)
    metadata = json.loads(content)
    snapshot = next(
        s
        for s in metadata["snapshots"]
        if s["snapshot-id"] == metadata["current-snapshot-id"]
    )
    schema_id = snapshot["schema-id"]
    schema = next(s for s in metadata["schemas"] if s["schema-id"] == schema_id)
    assert all(f["name"] != field["name"] for f in schema["fields"]), field
    assert all(f["id"] != field["id"] for f in schema["fields"]), field
    schema["fields"].append(field)
    metadata["last-column-id"] = max(metadata["last-column-id"], field["id"])
    put_s3_object(started_cluster, bucket, key, json.dumps(metadata).encode())

    _, _, manifest_list = get_s3_object(started_cluster, snapshot["manifest-list"])
    reader = avro.datafile.DataFileReader(
        io.BytesIO(manifest_list), avro.io.DatumReader()
    )
    manifest_paths = [record["manifest_path"] for record in reader]
    reader.close()
    assert manifest_paths, snapshot["manifest-list"]

    for path in manifest_paths:
        manifest_bucket, manifest_key, manifest = get_s3_object(started_cluster, path)
        reader = avro.datafile.DataFileReader(
            io.BytesIO(manifest), avro.io.DatumReader()
        )
        writer_schema = reader.datum_reader.writers_schema
        meta = dict(reader.meta)
        records = list(reader)
        reader.close()

        embedded = json.loads(meta["schema"])
        assert embedded["schema-id"] == schema_id, path
        embedded["fields"].append(field)
        meta["schema"] = json.dumps(embedded).encode()

        buffer = io.BytesIO()
        writer = avro.datafile.DataFileWriter(
            buffer, avro.io.DatumWriter(), writer_schema
        )
        for name, value in meta.items():
            if not name.startswith("avro."):
                writer.set_meta(name, value)
        for record in records:
            writer.append(record)
        writer.flush()
        put_s3_object(started_cluster, manifest_bucket, manifest_key, buffer.getvalue())
        writer.close()


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


def drop_struct_field_from_data_file(started_cluster, s3_uri, column, field):
    """Rewrite the data file at `s3_uri` in place, keeping `column` but without its `field`."""
    assert s3_uri.startswith("s3://"), s3_uri
    bucket, key = s3_uri[len("s3://") :].split("/", 1)

    response = started_cluster.minio_client.get_object(bucket, key)
    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()

    parquet_table = pq.read_table(pa.BufferReader(content))
    position = parquet_table.column_names.index(column)
    struct_field = parquet_table.field(position)
    struct = parquet_table.column(column).combine_chunks()
    assert field in struct_field.type.names, s3_uri

    # Carry the kept children as their own `pa.Field` objects, so each keeps the
    # `PARQUET:field_id` metadata the Iceberg reader resolves them by.
    kept = [f for f in struct_field.type if f.name != field]
    rebuilt = pa.StructArray.from_arrays(
        [struct.field(f.name) for f in kept],
        fields=kept,
        mask=struct.is_null() if struct.null_count else None,
    )
    parquet_table = parquet_table.set_column(
        position,
        pa.field(column, pa.struct(kept), nullable=struct_field.nullable, metadata=struct_field.metadata),
        rebuilt,
    )
    assert parquet_table.field(position).type.names == [f.name for f in kept], s3_uri

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
        "1\t('x',NULL)\n2\t('y',NULL)",
    )

    assert instance.query(f"SELECT s FROM {table_expression} WHERE id = 99").strip() == ""
    assert instance.query(f"SELECT count() FROM {table_expression}").strip() == "2"
    assert (
        instance.query(
            f"SELECT s, count() FROM {table_expression} GROUP BY s ORDER BY s"
        ).strip()
        == "('x',NULL)\t1\n('y',NULL)\t1"
    )


def create_nested_table(catalog, started_cluster, identifier, location, schema, spec, rows, arrow_schema, drop):
    """A nested-identity table whose data files do not store `drop`, so its value is only in the manifest."""
    table = catalog.create_table(
        identifier=identifier, schema=schema, location=location, partition_spec=spec
    )
    table.append(pa.Table.from_pylist(rows, schema=arrow_schema))
    files = data_file_paths(table)
    assert len(files) == len(rows)
    for path in files:
        drop_column_from_data_file(started_cluster, path, drop)
    return table


def test_identity_partition_column_nested_field_non_parquet_format(
    started_cluster_iceberg_no_spark,
):
    """A table-level format other than Parquet collapses `s.c` to `s` in the reader header.

    The data files hold Parquet bytes behind a declared `format = ORC`, because no writer available
    here produces a nested-identity-partitioned table with ORC data files. That does not weaken the
    arm: the declared format alone decides whether the header carries `s.c` or `s`, while the reader
    for each file is chosen from the manifest, so a genuinely ORC-backed table takes this branch too.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    prefix = f"data/{namespace}"

    create_nested_table(
        catalog,
        started_cluster_iceberg_no_spark,
        f"{namespace}.t_nested_orc",
        f"s3://warehouse-rest/{prefix}",
        NESTED_SCHEMA,
        PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="s.c"),
        ),
        [{"id": 1, "s": {"c": "x", "d": 10}}, {"id": 2, "s": {"c": "y", "d": 20}}],
        NESTED_ARROW_SCHEMA,
        "s",
    )

    for declared_format in ["ORC", "Avro", "Native"]:
        table_expression = (
            f"icebergS3(s3, filename = '{prefix}/', format={declared_format},"
            f" url = 'http://minio1:9001/warehouse-rest/')"
        )
        assert (
            instance.query(f"SELECT id, s.c, s.d FROM {table_expression} ORDER BY id").strip()
            == "1\tx\t\\N\n2\ty\t\\N"
        ), declared_format
        assert (
            instance.query(f"SELECT id, s FROM {table_expression} ORDER BY id").strip()
            == "1\t('x',NULL)\n2\t('y',NULL)"
        ), declared_format
        assert (
            instance.query(f"SELECT id FROM {table_expression} WHERE s.c = 'x' ORDER BY id").strip()
            == "1"
        ), declared_format


def test_identity_partition_column_nested_field_on_cluster(
    started_cluster_iceberg_no_spark,
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    prefix = f"data/{namespace}"

    create_nested_table(
        catalog,
        started_cluster_iceberg_no_spark,
        f"{namespace}.t_nested_cluster",
        f"s3://warehouse-rest/{prefix}",
        NESTED_SCHEMA,
        PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="s.c"),
        ),
        [{"id": 1, "s": {"c": "x", "d": 10}}, {"id": 2, "s": {"c": "y", "d": 20}}],
        NESTED_ARROW_SCHEMA,
        "s",
    )

    table_expression = (
        f"icebergS3Cluster('cluster_simple', s3, filename = '{prefix}/', format=Parquet,"
        f" url = 'http://minio1:9001/warehouse-rest/')"
    )
    assert (
        instance.query(f"SELECT id, s.c, s.d FROM {table_expression} ORDER BY id").strip()
        == "1\tx\t\\N\n2\ty\t\\N"
    )
    assert (
        instance.query(f"SELECT id, s FROM {table_expression} ORDER BY id").strip()
        == "1\t('x',NULL)\n2\t('y',NULL)"
    )


def test_identity_partition_column_two_nested_fields_under_one_parent(
    started_cluster_iceberg_no_spark,
):
    """Two identity sources inside one struct: both values must reach the same rebuilt tuple."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    create_nested_table(
        catalog,
        started_cluster_iceberg_no_spark,
        f"{namespace}.t_two_leaves",
        "s3://warehouse-rest/data",
        NESTED_SCHEMA,
        PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="p_c"),
            PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="p_d"),
        ),
        [{"id": 1, "s": {"c": "x", "d": 10}}, {"id": 2, "s": {"c": "y", "d": 20}}],
        NESTED_ARROW_SCHEMA,
        "s",
    )

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_two_leaves`"

    assert (
        instance.query(f"SELECT id, s FROM {table_expression} ORDER BY id").strip()
        == "1\t('x',10)\n2\t('y',20)"
    )
    assert (
        instance.query(f"SELECT id, s.c, s.d FROM {table_expression} ORDER BY id").strip()
        == "1\tx\t10\n2\ty\t20"
    )


def test_identity_partition_column_nested_field_sibling_kept_in_data_files(
    started_cluster_iceberg_no_spark,
):
    """Only the partition leaf is missing from the data files, so the sibling must come from them.

    Every other nested arm drops the whole parent column, where a sibling's file value and the type
    default are both NULL. Here `s.d` holds 10 and 20, so a rebuilt tuple that took its non-partition
    elements from defaults instead of from the file would differ.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    prefix = f"data/{namespace}"

    table = catalog.create_table(
        identifier=f"{namespace}.t_nested_sibling",
        schema=NESTED_SCHEMA,
        location=f"s3://warehouse-rest/{prefix}",
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
        drop_struct_field_from_data_file(started_cluster_iceberg_no_spark, path, "s", "c")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_nested_sibling`"

    assert (
        instance.query(f"SELECT id, s FROM {table_expression} ORDER BY id").strip()
        == "1\t('x',10)\n2\t('y',20)"
    )
    assert (
        instance.query(f"SELECT id, s.c, s.d FROM {table_expression} ORDER BY id").strip()
        == "1\tx\t10\n2\ty\t20"
    )

    # A table-level format other than Parquet leaves the parent tuple as the only header column,
    # which is where taking the other elements from defaults would be least visible.
    orc_expression = (
        f"icebergS3(s3, filename = '{prefix}/', format=ORC,"
        f" url = 'http://minio1:9001/warehouse-rest/')"
    )
    assert (
        instance.query(f"SELECT id, s FROM {orc_expression} ORDER BY id").strip()
        == "1\t('x',10)\n2\t('y',20)"
    )


DEEP_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="s", field_type=StructType(
        NestedField(field_id=3, name="t", field_type=StructType(
            NestedField(field_id=4, name="c", field_type=StringType(), required=False),
            NestedField(field_id=5, name="e", field_type=LongType(), required=False),
        ), required=False),
    ), required=False),
)

DEEP_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field(
            "s",
            pa.struct(
                [
                    pa.field(
                        "t",
                        pa.struct(
                            [
                                pa.field("c", pa.string(), nullable=True),
                                pa.field("e", pa.int64(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    )
                ]
            ),
            nullable=True,
        ),
    ]
)


def create_deep_table(started_cluster, catalog, namespace, name):
    return create_nested_table(
        catalog,
        started_cluster,
        f"{namespace}.{name}",
        "s3://warehouse-rest/data",
        DEEP_SCHEMA,
        PartitionSpec(
            PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="s.t.c"),
        ),
        [
            {"id": 1, "s": {"t": {"c": "x", "e": 10}}},
            {"id": 2, "s": {"t": {"c": "y", "e": 20}}},
        ],
        DEEP_ARROW_SCHEMA,
        "s",
    )


def test_identity_partition_column_nested_field_intermediate_ancestor(
    started_cluster_iceberg_no_spark,
):
    """Requesting an ancestor alongside the leaf leaves only the intermediate tuple in the header."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    create_deep_table(started_cluster_iceberg_no_spark, catalog, namespace, "t_deep")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_deep`"

    assert (
        instance.query(f"SELECT id, s.t, s.t.c FROM {table_expression} ORDER BY id").strip()
        == "1\t('x',NULL)\tx\n2\t('y',NULL)\ty"
    )
    assert (
        instance.query(f"SELECT id, s.t.c FROM {table_expression} ORDER BY id").strip()
        == "1\tx\n2\ty"
    )


def test_identity_partition_column_nested_field_in_reader_prewhere(
    started_cluster_iceberg_no_spark,
):
    """A PREWHERE over the parent tuple is evaluated inside the reader, before the projection."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    create_deep_table(started_cluster_iceberg_no_spark, catalog, namespace, "t_deep_prewhere")

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_deep_prewhere`"

    # Both prewhere settings are pinned: either one off disables prewhere, and the arm would then
    # stop exercising the in-reader path while still passing.
    settings = {
        "optimize_functions_to_subcolumns": 0,
        "optimize_move_to_prewhere": 1,
        "query_plan_optimize_prewhere": 1,
    }
    assert (
        instance.query(
            f"SELECT id FROM {table_expression}"
            f" PREWHERE tupleElement(tupleElement(s, 't'), 'c') = 'x' ORDER BY id",
            settings=settings,
        ).strip()
        == "1"
    )


ARRAY_ELEMENT_STRUCT = StructType(
    NestedField(field_id=7, name="b", field_type=LongType(), required=False),
)

ARROW_ARRAY_OF_STRUCT = pa.list_(
    pa.field("element", pa.struct([pa.field("b", pa.int64(), nullable=True)]), nullable=True)
)

# A top-level identity source named `a.b`, whose flattened name the descendant of the array of
# structs beside it also spells, so `a.b` denotes two columns rather than one.
TOP_DOTTED_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="a", field_type=ListType(
        element_id=6, element=ARRAY_ELEMENT_STRUCT, element_required=False), required=False),
    NestedField(field_id=3, name="a.b", field_type=LongType(), required=False),
)

TOP_DOTTED_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("a", ARROW_ARRAY_OF_STRUCT, nullable=True),
        pa.field("a.b", pa.int64(), nullable=True),
    ]
)

def test_identity_partition_column_top_level_dotted_name_shared_with_array_descendant(
    started_cluster_iceberg_no_spark,
):
    """A non-regression arm: green without any fix, and it must stay green.

    The partition source is the top-level column named `a.b`, whose flattened name the descendant
    of the array of structs beside it also spells. A top-level name always denotes its top-level
    column, because an identifier lookup resolves a column before a subcolumn, so this value is
    still injected. Do not "fix" this arm by inverting the expectation.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)

    create_nested_table(
        catalog,
        started_cluster_iceberg_no_spark,
        f"{namespace}.t_top_dotted",
        "s3://warehouse-rest/data",
        TOP_DOTTED_SCHEMA,
        PartitionSpec(
            PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="p_top"),
        ),
        [
            {"id": 1, "a": [{"b": 11}], "a.b": 100},
            {"id": 2, "a": [{"b": 22}], "a.b": 200},
        ],
        TOP_DOTTED_ARROW_SCHEMA,
        "a_x2Eb",
    )

    create_clickhouse_iceberg_database(instance, CATALOG_NAME)
    table_expression = f"{CATALOG_NAME}.`{namespace}.t_top_dotted`"

    assert (
        instance.query(f"SELECT id, `a.b`, a FROM {table_expression} ORDER BY id").strip()
        == "1\t100\t[(11)]\n2\t200\t[(22)]"
    )


def test_identity_partition_column_nested_field_flat_name_shared_with_top_level_column(
    started_cluster_iceberg_no_spark,
):
    """The partition leaf `s.c` and an unrelated top-level column named `s.c` share a flattened name.

    Such a name denotes neither of them, so the leaf may only be injected through its parent tuple.
    Injecting it under the shared name instead would replace the top-level column's own data with the
    partition value.

    pyiceberg refuses to write this schema, so the colliding field is appended to the snapshot's
    schema after the write. The table is read through `icebergS3` rather than the catalog, which
    derives a table's schema from the metadata inlined in its response instead of from the copy in
    storage that carries the appended field.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(started_cluster_iceberg_no_spark)
    prefix = f"data/{namespace}"

    table = catalog.create_table(
        identifier=f"{namespace}.t_nested_shared",
        schema=NESTED_SCHEMA,
        location=f"s3://warehouse-rest/{prefix}",
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
        drop_struct_field_from_data_file(started_cluster_iceberg_no_spark, path, "s", "c")

    append_field_to_snapshot_schema(
        started_cluster_iceberg_no_spark,
        table,
        {"id": 5, "name": "s.c", "required": False, "type": "string"},
    )

    table_expression = (
        f"icebergS3(s3, filename = '{prefix}/', format=Parquet,"
        f" url = 'http://minio1:9001/warehouse-rest/')"
    )

    assert (
        instance.query(f"SELECT id, s FROM {table_expression} ORDER BY id").strip()
        == "1\t('x',10)\n2\t('y',20)"
    )
    assert (
        instance.query(f"SELECT id, `s.c` FROM {table_expression} ORDER BY id").strip()
        == "1\t\\N\n2\t\\N"
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
