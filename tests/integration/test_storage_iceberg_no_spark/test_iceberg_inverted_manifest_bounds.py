"""A manifest that declares a lower bound above the upper bound must not prune the data file it
describes, so a filtered read still returns every matching row. The fallback is per column: a
well-formed column in the same manifest entry must keep pruning."""

import decimal
import os
import shutil
import tempfile

import avro.datafile
import avro.io
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import DecimalType, LongType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    get_uuid_str,
)

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}


class ContainerManifests:
    """The metadata directory of a table stored on the ClickHouse node's own filesystem."""

    def __init__(self, instance, table_path):
        self.instance = instance
        self.table_path = table_path

    def list(self):
        return self.instance.get_files_list_in_container(f"{self.table_path}/metadata")

    def get(self, remote_path, local_path):
        self.instance.copy_file_from_container(remote_path, local_path)

    def put(self, local_path, remote_path):
        self.instance.copy_file_to_container(local_path, remote_path)


class MinioManifests:
    """The metadata directory of a table stored in MinIO under one key prefix."""

    def __init__(self, minio_client, bucket, key_prefix):
        self.minio_client = minio_client
        self.bucket = bucket
        self.key_prefix = key_prefix

    def list(self):
        return [
            obj.object_name
            for obj in self.minio_client.list_objects(
                self.bucket, prefix=f"{self.key_prefix}/metadata", recursive=True
            )
        ]

    def get(self, remote_path, local_path):
        self.minio_client.fget_object(self.bucket, remote_path, local_path)

    def put(self, local_path, remote_path):
        self.minio_client.fput_object(self.bucket, remote_path, local_path)


def _pruned_files(instance, table_name, select_expression):
    common = {
        "input_format_parquet_bloom_filter_push_down": 0,
        "input_format_parquet_filter_push_down": 0,
        **NOCACHE,
    }
    return check_validity_and_get_prunned_files_general(
        instance,
        table_name,
        {"use_iceberg_partition_pruning": 0, **common},
        {"use_iceberg_partition_pruning": 1, **common},
        "IcebergMinMaxIndexPrunedFiles",
        select_expression,
    )


def _swap_one_field(lower_bounds, upper_bounds, field_id):
    """Exchange the bound stored under `field_id` between the two maps, leaving every other column
    untouched. Returns whether the id was present in both."""
    lower = next((entry for entry in lower_bounds if entry["key"] == field_id), None)
    upper = next((entry for entry in upper_bounds if entry["key"] == field_id), None)
    if lower is None or upper is None:
        return False
    lower["value"], upper["value"] = upper["value"], lower["value"]
    return True


def _set_one_field(lower_bounds, upper_bounds, field_id, lower_raw, upper_raw):
    """Overwrite the bound stored under `field_id` in both maps with a raw two's-complement
    big-endian payload, leaving every other column untouched. Returns whether the id was present in
    both."""
    lower = next((entry for entry in lower_bounds if entry["key"] == field_id), None)
    upper = next((entry for entry in upper_bounds if entry["key"] == field_id), None)
    if lower is None or upper is None:
        return False
    lower["value"], upper["value"] = lower_raw, upper_raw
    return True


def _patch_manifests(manifests, patch_data_file):
    """Apply `patch_data_file` to every manifest entry of the table that carries both bound maps, and
    rewrite the manifests it changed. Returns the number of rewritten entries, which the caller
    asserts is non-zero.

    Bound maps are keyed by Iceberg field id, which the ClickHouse writer assigns sequentially from
    1 in declaration order and a pyiceberg schema states outright. Either way an id fails closed
    rather than silently: one that does not exist leaves `patched` at 0, and one naming the wrong
    column reddens the pruning assertion of the column that was supposed to stay well formed."""
    patched = 0
    temp_dir = tempfile.mkdtemp()
    try:
        for remote_path in manifests.list():
            if not remote_path.endswith(".avro"):
                continue

            local_path = os.path.join(temp_dir, os.path.basename(remote_path))
            manifests.get(remote_path, local_path)

            with open(local_path, "rb") as f:
                reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
                schema = reader.datum_reader.writers_schema
                metadata = dict(reader.meta)
                records = list(reader)
                reader.close()

            patched_here = 0
            for record in records:
                data_file = record.get("data_file")
                # Manifest-list entries carry manifest_path instead of data_file.
                if not isinstance(data_file, dict):
                    continue
                if data_file.get("lower_bounds") is None or data_file.get("upper_bounds") is None:
                    continue
                if patch_data_file(data_file):
                    patched_here += 1

            if patched_here == 0:
                continue

            with open(local_path, "wb") as f:
                writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
                for key, value in metadata.items():
                    if not key.startswith("avro."):
                        writer.set_meta(key, value)
                for record in records:
                    writer.append(record)
                writer.close()

            manifests.put(local_path, remote_path)
            patched += patched_here
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return patched


def _swap_bounds_in_manifests(manifests, only_field_id=None):
    """Exchange `data_file.lower_bounds` with `data_file.upper_bounds` in every manifest of the
    table. With `only_field_id`, exchange just that one field id and leave the other columns well
    formed."""

    def swap(data_file):
        if only_field_id is None:
            data_file["lower_bounds"], data_file["upper_bounds"] = (
                data_file["upper_bounds"],
                data_file["lower_bounds"],
            )
            return True
        return _swap_one_field(
            data_file["lower_bounds"], data_file["upper_bounds"], only_field_id
        )

    return _patch_manifests(manifests, swap)


def _set_bounds_in_manifests(manifests, field_id, lower_raw, upper_raw):
    """Replace both bounds of one column in every manifest of the table with raw payloads, which is
    how a manifest spells a value outside the column's declared precision."""
    return _patch_manifests(
        manifests,
        lambda data_file: _set_one_field(
            data_file["lower_bounds"],
            data_file["upper_bounds"],
            field_id,
            lower_raw,
            upper_raw,
        ),
    )


def test_iceberg_inverted_manifest_bounds(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_inverted_manifest_bounds_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    instance.query(
        f"CREATE TABLE {table_name} (id Int64, s String) "
        f"ENGINE = IcebergLocal('{table_path}/', 'Parquet')",
        settings=NOCACHE,
    )
    # One data file per INSERT, with disjoint ranges in both columns so that a filter on either can
    # prune independently of the other.
    for k in range(4):
        instance.query(
            f"INSERT INTO {table_name} SELECT number, toString(number) "
            f"FROM numbers({k * 1000}, 100)",
            settings=NOCACHE,
        )
    assert instance.query(f"SELECT count() FROM {table_name}", settings=NOCACHE).strip() == "400"
    instance.query(f"DROP TABLE {table_name}")

    def select(where):
        return (
            f"SELECT id, s FROM icebergLocal(local, path = '{table_path}/') "
            f"WHERE {where} ORDER BY ALL"
        )

    # Only the first data file holds ids below 10, and only its strings sort below "1"; the other
    # three files start at "1000", "2000" and "3000".
    id_expr = select("id < 10")
    s_expr = select("s < '1'")

    def pruned_files(select_expression):
        return _pruned_files(instance, table_name, select_expression)

    manifests = ContainerManifests(instance, table_path)

    # Control: with well-formed bounds each filter prunes the three non-matching files. Without it,
    # a fix that stopped pruning altogether would satisfy the assertions below.
    assert pruned_files(id_expr) == 3
    assert pruned_files(s_expr) == 3

    # Invert `id` alone. Its bounds now describe an empty range, so pruning on them would drop the
    # file holding matching rows and no file may be pruned; `s` is still well formed in the same
    # manifest entry and must keep pruning. Together these pin the fallback as per column rather
    # than a manifest-wide loss of min/max pruning.
    assert _swap_bounds_in_manifests(manifests, only_field_id=1) > 0
    assert pruned_files(id_expr) == 0
    assert pruned_files(s_expr) == 3

    # Invert `s` as well, so no column has usable bounds. The fallback holds whichever column is
    # inverted, and neither filter may prune.
    assert _swap_bounds_in_manifests(manifests, only_field_id=2) > 0
    assert pruned_files(s_expr) == 0
    assert pruned_files(id_expr) == 0


def test_iceberg_inverted_decimal_manifest_bounds(started_cluster_iceberg_no_spark):
    """Decimal bounds are decoded one integral unit outwards, so an inversion narrower than two
    integral units still orders after that shift and only the values as declared reveal it. A bound
    close enough to the edge of its type has no shifted form at all, which is a second way to lose
    the min/max condition, so both columns below are needed: one column for each.

    pyiceberg writes the table, so this exercises the S3 storage backend rather than the local one
    the sibling test above uses."""
    cluster = started_cluster_iceberg_no_spark
    instance = cluster.instances["node1"]
    table_name = "test_iceberg_inverted_decimal_bounds_" + get_uuid_str()
    key_prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    catalog = load_catalog(
        "demo",
        **{
            "uri": f"http://localhost:{cluster.iceberg_rest_catalog_port}",
            "type": "rest",
            "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )
    namespace = f"clickhouse_{get_uuid_str()}"
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        f"{namespace}.{table_name}",
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "d", DecimalType(10, 2), required=False),
            NestedField(3, "e", DecimalType(9, 1), required=False),
        ),
        location=f"s3://{cluster.minio_bucket}/{key_prefix}",
        partition_spec=PartitionSpec(),
    )

    # The field ids must reach the Parquet files for the Iceberg reader to match the columns.
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), True, metadata={b"PARQUET:field_id": b"1"}),
            pa.field("d", pa.decimal128(10, 2), True, metadata={b"PARQUET:field_id": b"2"}),
            pa.field("e", pa.decimal128(9, 1), True, metadata={b"PARQUET:field_id": b"3"}),
        ]
    )
    # One data file per append, with disjoint ranges in every column. Every `d` range spans 1.50,
    # which is below the two integral units the outward shift adds, so inverting it stays ordered
    # once shifted and is invisible to a check made after the shift.
    for lowest in (0, 10):
        table.append(
            pa.Table.from_pylist(
                [
                    {
                        "id": lowest + 1,
                        "d": decimal.Decimal(f"{lowest}.00"),
                        "e": decimal.Decimal(f"{lowest}.1"),
                    },
                    {
                        "id": lowest + 2,
                        "d": decimal.Decimal(f"{lowest + 1}.50"),
                        "e": decimal.Decimal(f"{lowest + 1}.1"),
                    },
                ],
                schema=arrow_schema,
            )
        )

    source = (
        f"icebergS3(s3, filename = '{key_prefix}/', format=Parquet, "
        f"url = 'http://minio1:9001/{cluster.minio_bucket}/')"
    )
    assert (
        instance.query(f"SELECT count() FROM {source}", settings=NOCACHE).strip() == "4"
    )

    def pruned_files(where):
        return _pruned_files(
            instance,
            table_name,
            f"SELECT id, d FROM {source} WHERE {where} ORDER BY ALL",
        )

    # Only the first data file holds ids below 5 and decimals below 0.25. The literal is typed: a
    # bare 0.25 is a Float64 and leaves the Decimal column without a usable min/max condition, so
    # nothing would prune and every assertion below would hold whatever the bounds said.
    id_expr = "id < 5"
    d_expr = "d < toDecimal64(0.25, 2)"

    # Control: with well-formed bounds each filter prunes the one non-matching file.
    assert pruned_files(id_expr) == 1
    assert pruned_files(d_expr) == 1

    # Invert `d` alone. Its declared bounds now describe an empty range, so no file may be pruned on
    # them, while `id` is still well formed in the same manifest entry and must keep pruning.
    manifests = MinioManifests(cluster.minio_client, cluster.minio_bucket, key_prefix)
    assert _swap_bounds_in_manifests(manifests, only_field_id=2) > 0
    assert pruned_files(d_expr) == 0
    assert pruned_files(id_expr) == 1

    # `e` is a Decimal32, so its unscaled bound is an Int32. A bound out of the column's declared
    # precision is still a legal encoding, and nothing rejects one: the pair below is ordered as
    # declared, yet shifting the upper bound one integral unit outwards leaves Int32. A bound with no
    # shifted form gives the column no min/max condition at all, so again nothing may be pruned on it.
    e_expr = "e < toDecimal32(0.5, 1)"
    assert pruned_files(e_expr) == 1
    assert _set_bounds_in_manifests(manifests, 3, b"\x7f\xff\xff\xf0", b"\x7f\xff\xff\xfa") > 0
    assert pruned_files(e_expr) == 0
    assert pruned_files(id_expr) == 1
