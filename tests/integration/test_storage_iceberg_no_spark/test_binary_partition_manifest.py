#!/usr/bin/env python3

import io
import json
import uuid

import avro.datafile
import avro.io
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import ManifestContent
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import BinaryType, LongType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import get_uuid_str

BASE_URL = "http://rest:8181/v1"

WRITE_SETTINGS = {
    "allow_experimental_iceberg_compaction": 1,
    "iceberg_manifest_min_count_to_compact": 1,
    "allow_insert_into_iceberg": 1,
    "write_full_path_in_iceberg_metadata": 1,
}

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("p", pa.binary(), nullable=True),
    ]
)


def load_catalog_impl(started_cluster):
    return load_catalog(
        "demo",
        **{
            "uri": f"http://localhost:{started_cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def partition_avro_types(started_cluster, table):
    """The Avro type of the partition field `p` declared by each manifest of the current snapshot.
    Reads only the file header, so it also works on manifests whose records cannot be decoded."""
    result = {}
    for manifest in table.current_snapshot().manifests(table.io):
        bucket, key = manifest.manifest_path[len("s3://") :].split("/", 1)
        response = started_cluster.minio_client.get_object(bucket, key)
        try:
            content = response.read()
        finally:
            response.close()
            response.release_conn()
        reader = avro.datafile.DataFileReader(io.BytesIO(content), avro.io.DatumReader())
        avro_schema = json.loads(reader.meta["avro.schema"])
        reader.close()
        [data_file] = [field for field in avro_schema["fields"] if field["name"] == "data_file"]
        [partition] = [field for field in data_file["type"]["fields"] if field["name"] == "partition"]
        [partition_field] = [field for field in partition["type"]["fields"] if field["name"] == "p"]
        result[manifest.manifest_path] = partition_field["type"]
    return result


def test_binary_partition_manifest(started_cluster_iceberg_no_spark):
    """ClickHouse reads both Iceberg `binary` and `string` as `String`, but a `binary` partition value
    is an Avro `bytes` in the manifest, and an Avro `string` must be valid UTF-8. Manifests written by
    ClickHouse must keep `bytes`, or other readers cannot decode a non-UTF-8 partition value."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    namespace = f"clickhouse_{uuid.uuid4()}"
    catalog = load_catalog_impl(cluster)

    identifier = f"{namespace}.t_binary_partition"
    table = catalog.create_table(
        identifier=identifier,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="p", field_type=BinaryType(), required=False),
        ),
        location=f"s3://warehouse-rest/{namespace}",
        partition_spec=PartitionSpec(
            PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="p")
        ),
    )
    # One manifest per append, two of them for the 0xFF partition.
    for row in [(1, b"\xff"), (2, b"\xff"), (3, b"\x01\x02")]:
        table.append(pa.Table.from_pylist([{"id": row[0], "p": row[1]}], schema=ARROW_SCHEMA))

    database = f"demo_{get_uuid_str()}"
    instance.query(
        f"""
SET allow_database_iceberg=true;
SET write_full_path_in_iceberg_metadata=1;
CREATE DATABASE {database} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS catalog_type='rest', warehouse='demo', storage_endpoint='http://minio1:9001/warehouse-rest'
    """
    )
    table_expression = f"{database}.`{identifier}`"

    def scan_rows():
        return sorted((row["id"], row["p"]) for row in catalog.load_table(identifier).scan().to_arrow().to_pylist())

    expected_rows = [(1, b"\xff"), (2, b"\xff"), (3, b"\x01\x02")]
    assert scan_rows() == expected_rows

    # `OPTIMIZE TABLE ... MANIFEST` rewrites every manifest.
    table = catalog.load_table(identifier)
    snapshot_before = table.current_snapshot().snapshot_id
    before = partition_avro_types(cluster, table)
    assert len(before) == 3 and all(t == ["null", "bytes"] for t in before.values()), before

    instance.query(f"OPTIMIZE TABLE {table_expression} MANIFEST", settings=WRITE_SETTINGS)

    table = catalog.load_table(identifier)
    assert table.current_snapshot().snapshot_id != snapshot_before
    after_optimize = partition_avro_types(cluster, table)
    assert 0 < len(after_optimize) < len(before), after_optimize
    assert not set(after_optimize) & set(before), after_optimize
    assert all(t == ["null", "bytes"] for t in after_optimize.values()), after_optimize
    assert scan_rows() == expected_rows

    # INSERT writes a new manifest.
    instance.query(f"INSERT INTO {table_expression} VALUES (4, 'ok')", settings=WRITE_SETTINGS)

    table = catalog.load_table(identifier)
    after_insert = partition_avro_types(cluster, table)
    new_manifests = {path: t for path, t in after_insert.items() if path not in after_optimize}
    assert list(new_manifests.values()) == [["null", "bytes"]], after_insert
    assert scan_rows() == expected_rows + [(4, b"ok")]
    assert (
        instance.query(f"SELECT id, hex(p) FROM {table_expression} ORDER BY id").strip()
        == "1\tFF\n2\tFF\n3\t0102\n4\t6F6B"
    )

    # A DELETE writes a position-delete manifest.
    instance.query(f"DELETE FROM {table_expression} WHERE id = 4", settings=WRITE_SETTINGS)

    table = catalog.load_table(identifier)
    after_delete = partition_avro_types(cluster, table)
    delete_manifests = [
        manifest.manifest_path
        for manifest in table.current_snapshot().manifests(table.io)
        if manifest.content == ManifestContent.DELETES
    ]
    assert len(delete_manifests) == 1 and delete_manifests[0] not in after_insert, after_delete
    assert all(t == ["null", "bytes"] for t in after_delete.values()), after_delete
    assert scan_rows() == expected_rows
    assert (
        instance.query(f"SELECT id, hex(p) FROM {table_expression} ORDER BY id").strip()
        == "1\tFF\n2\tFF\n3\t0102"
    )
