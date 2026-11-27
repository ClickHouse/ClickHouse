"""An Iceberg `time` column is exposed as `Int64` microseconds since midnight. The manifest bounds,
partition values and ORC/Avro data files all carry that number, so the Parquet data files written by a
spec writer (TIME(MICROS)) must be decoded to it as well: otherwise `WHERE t = <value>` is pruned
against bounds in another unit and the shown value depends on the partition spec."""

import datetime
import re
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import ListType, LongType, MapType, NestedField, StructType, TimeType

from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    get_uuid_str,
)

SETTINGS = {"use_iceberg_metadata_files_cache": False, "use_parquet_metadata_cache": False}
NO_PARQUET_PUSH_DOWN = {
    "input_format_parquet_filter_push_down": 0,
    "input_format_parquet_page_filter_push_down": 0,
    "input_format_parquet_bloom_filter_push_down": 0,
}

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "t", TimeType(), required=False),
    NestedField(3, "s", StructType(NestedField(5, "tt", TimeType(), required=False)), required=False),
    NestedField(4, "l", ListType(6, TimeType(), element_required=False), required=False),
)


def field(name, arrow_type, field_id, nullable=True):
    return pa.field(name, arrow_type, nullable, metadata={b"PARQUET:field_id": str(field_id).encode()})


ARROW_SCHEMA = pa.schema(
    [
        field("id", pa.int64(), 1, nullable=False),
        field("t", pa.time64("us"), 2),
        field("s", pa.struct([field("tt", pa.time64("us"), 5)]), 3),
        field("l", pa.list_(field("element", pa.time64("us"), 6)), 4),
    ]
)


def time_of(i):
    return datetime.time(i % 3, 30, 0, 250000)


def micros_of(i):
    return ((i % 3) * 3600 + 30 * 60) * 1_000_000 + 250000


def rows(ids):
    return pa.Table.from_pydict(
        {
            "id": list(ids),
            "t": [time_of(i) for i in ids],
            "s": [{"tt": time_of(i)} for i in ids],
            "l": [[time_of(i)] for i in ids],
        },
        schema=ARROW_SCHEMA,
    )


def expected(ids, nested=True):
    return "\n".join(
        f"{i}\t{micros_of(i)}\t{micros_of(i)}\t[{micros_of(i)}]" if nested else f"{i}\t{micros_of(i)}"
        for i in ids
    )


class Tables:
    def __init__(self, cluster):
        self.cluster = cluster
        self.instance = cluster.instances["node1"]
        self.catalog = load_catalog(
            "demo",
            **{
                "uri": f"http://localhost:{cluster.iceberg_rest_catalog_port}",
                "type": "rest",
                "s3.endpoint": f"http://{cluster.minio_ip}:{cluster.minio_port}",
                "s3.access-key-id": minio_access_key,
                "s3.secret-access-key": minio_secret_key,
            },
        )
        self.namespace = f"clickhouse_{get_uuid_str()}"
        self.catalog.create_namespace(self.namespace)

    def create(self, prefix, schema=SCHEMA, spec=PartitionSpec()):
        table_name = f"{prefix}_{get_uuid_str()}"
        key_prefix = f"var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
        table = self.catalog.create_table(
            f"{self.namespace}.{table_name}",
            schema=schema,
            location=f"s3://{self.cluster.minio_bucket}/{key_prefix}",
            partition_spec=spec,
        )
        return table_name, key_prefix, table

    def source(self, key_prefix, format="Parquet"):
        return (
            f"icebergS3(s3, filename = '{key_prefix}/', format = {format}, "
            f"url = 'http://minio1:9001/{self.cluster.minio_bucket}/')"
        )

    def query(self, query, settings={}):
        return self.instance.query(query, settings={**SETTINGS, **settings}).strip()


def write_parquet(table, name, arrow_table, **write_options):
    path = f"{table.location()}/data/{name}-{uuid.uuid4()}.parquet"
    with table.io.new_output(path).create() as out:
        pq.write_table(arrow_table, out, **write_options)
    return path, len(table.io.new_input(path))


def test_iceberg_time_column_reads_microseconds(started_cluster_iceberg_no_spark):
    tables = Tables(started_cluster_iceberg_no_spark)

    # One file per distinct value, so that min/max pruning has files to drop.
    flat_name, flat, table = tables.create("time_flat")
    for k in range(3):
        table.append(rows([i for i in range(8) if i % 3 == k]))

    tpart_name, tpart, table = tables.create(
        "time_identity", spec=PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "t_part"))
    )
    table.append(rows(range(8)))

    # pyiceberg 0.11.1 cannot `append` to a `bucket` partition of a `time` column (it passes the bucket
    # number to `time_to_micros`), so the files of the three buckets are committed by hand.
    tbucket_name, tbucket, table = tables.create(
        "time_bucket", spec=PartitionSpec(PartitionField(2, 1000, BucketTransform(4), "t_bucket"))
    )
    bucket_of = BucketTransform(4).transform(TimeType())
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append:
            for k in range(3):
                ids = [i for i in range(8) if i % 3 == k]
                path, size = write_parquet(table, f"bucket-{k}", rows(ids))
                append.append_data_file(
                    DataFile.from_args(
                        _table_format_version=2,
                        content=DataFileContent.DATA,
                        file_path=path,
                        file_format=FileFormat.PARQUET,
                        partition=Record(bucket_of(micros_of(k))),
                        record_count=len(ids),
                        file_size_in_bytes=size,
                        spec_id=0,
                    )
                )

    # A Parquet file without field ids, matched to the table schema by name.
    _, tadded, table = tables.create(
        "time_added",
        schema=Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "t", TimeType()),
            NestedField(3, "s", StructType(NestedField(5, "tt", TimeType()))),
            NestedField(4, "l", ListType(6, TimeType(), element_required=False)),
            NestedField(7, "m", MapType(8, TimeType(), 9, LongType(), value_required=False)),
        ),
    )
    path, _ = write_parquet(
        table,
        "added",
        pa.Table.from_pydict(
            {
                "id": list(range(8)),
                "t": [time_of(i) for i in range(8)],
                "s": [{"tt": time_of(i)} for i in range(8)],
                "l": [[time_of(i)] for i in range(8)],
                "m": [[(time_of(i), i)] for i in range(8)],
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), False),
                    pa.field("t", pa.time64("us")),
                    pa.field("s", pa.struct([pa.field("tt", pa.time64("us"))])),
                    pa.field("l", pa.list_(pa.time64("us"))),
                    pa.field("m", pa.map_(pa.time64("us"), pa.int64())),
                ]
            ),
        ),
    )
    table.add_files([path])
    added_rows = "\n".join(
        f"{i}\t{micros_of(i)}\t{micros_of(i)}\t[{micros_of(i)}]\t{{{micros_of(i)}:{i}}}" for i in range(8)
    )

    matching = "1\n4\n7"
    nested_columns = "id, t, s.tt, l"
    for key_prefix, columns, expected_rows in (
        (flat, nested_columns, expected(range(8))),
        (tpart, nested_columns, expected(range(8))),
        (tbucket, nested_columns, expected(range(8))),
        (tadded, f"{nested_columns}, m", added_rows),
    ):
        source = tables.source(key_prefix)
        assert tables.query(f"SELECT {columns} FROM {source} ORDER BY id") == expected_rows, key_prefix
        for where in (f"t = {micros_of(1)}", "t >= 5400000000 AND t < 9000000000"):
            for settings in ({}, NO_PARQUET_PUSH_DOWN, {"use_iceberg_partition_pruning": 0}):
                assert tables.query(f"SELECT id FROM {source} WHERE {where} ORDER BY id", settings) == matching, (
                    key_prefix,
                    where,
                    settings,
                )

    # Every data file is Parquet while the table is configured as ORC.
    source = tables.source(flat, "ORC")
    assert tables.query(f"SELECT id, t FROM {source} ORDER BY id") == expected(range(8), nested=False)
    assert tables.query(f"SELECT id FROM {source} WHERE t = {micros_of(1)} ORDER BY id") == matching

    # The row assertions above make these non-vacuous: two empty results also compare equal.
    for table_name, key_prefix, event in (
        (flat_name, flat, "IcebergMinMaxIndexPrunedFiles"),
        (tpart_name, tpart, "IcebergPartitionPrunedFiles"),
        (tbucket_name, tbucket, "IcebergPartitionPrunedFiles"),
    ):
        pruned = check_validity_and_get_prunned_files_general(
            tables.instance,
            table_name,
            {**SETTINGS, "use_iceberg_partition_pruning": 0},
            {**SETTINGS, "use_iceberg_partition_pruning": 1},
            event,
            f"SELECT id, t FROM {tables.source(key_prefix)} WHERE t = {micros_of(1)} ORDER BY id",
        )
        assert pruned == 2, (key_prefix, event, pruned)

    # One file of three row groups with one distinct value each, committed without column bounds, so that
    # only the Parquet row-group statistics can skip data.
    _, tgroups, table = tables.create("time_row_groups")
    path, size = write_parquet(table, "row-groups", rows([0, 3, 6, 1, 4, 7, 2, 5]), row_group_size=3)
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append:
            append.append_data_file(
                DataFile.from_args(
                    _table_format_version=2,
                    content=DataFileContent.DATA,
                    file_path=path,
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=8,
                    file_size_in_bytes=size,
                    spec_id=0,
                )
            )
    query_id = f"time_row_groups_{uuid.uuid4()}"
    result = tables.instance.query(
        f"SELECT id FROM {tables.source(tgroups)} WHERE t = {micros_of(1)} ORDER BY id",
        settings={**SETTINGS, "use_iceberg_partition_pruning": 0},
        query_id=query_id,
    )
    assert result.strip() == matching
    tables.instance.query("SYSTEM FLUSH LOGS")
    pruned_row_groups = tables.query(
        f"SELECT ProfileEvents['ParquetPrunedRowGroups'] FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert pruned_row_groups == "2", pruned_row_groups


class DeleteManifestWriter(ManifestWriterV2):
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


def commit_equality_delete(table, keys):
    """pyiceberg 0.11.1 writes no equality deletes: the delete file and its manifest are written by hand."""
    path, size = write_parquet(table, "equality-delete", keys)
    delete_file = DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.EQUALITY_DELETES,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=keys.num_rows,
        file_size_in_bytes=size,
        equality_ids=[2],
        spec_id=0,
    )
    with table.transaction() as tx:
        append = tx.update_snapshot().fast_append()
        writer = DeleteManifestWriter(
            spec=tx.table_metadata.spec(),
            schema=tx.table_metadata.schema(),
            output_file=table.io.new_output(f"{table.location()}/metadata/{uuid.uuid4()}-m0.avro"),
            snapshot_id=append._snapshot_id,
            avro_compression="deflate",
        )
        with writer:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=append._snapshot_id,
                    sequence_number=None,
                    file_sequence_number=None,
                    data_file=delete_file,
                )
            )
        delete_manifest = writer.to_manifest_file()
        process_manifests = append._process_manifests
        append._process_manifests = lambda manifests: process_manifests(manifests) + [delete_manifest]
        append.commit()


def test_iceberg_time_column_equality_delete_and_compaction(started_cluster_iceberg_no_spark):
    tables = Tables(started_cluster_iceberg_no_spark)

    # Row 8 is at a whole second, which the key 03:00:00 matches whatever the unit of both sides is.
    for required in (False, True):
        _, eqdel, table = tables.create(
            "time_equality_delete",
            schema=Schema(NestedField(1, "id", LongType(), required=True), NestedField(2, "t", TimeType(), required=required)),
        )
        t_field = field("t", pa.time64("us"), 2, nullable=not required)
        table.append(
            pa.Table.from_pydict(
                {"id": list(range(9)), "t": [time_of(i) for i in range(8)] + [datetime.time(3, 0, 0)]},
                schema=pa.schema([field("id", pa.int64(), 1, nullable=False), t_field]),
            )
        )
        commit_equality_delete(
            table, pa.Table.from_pydict({"t": [time_of(1), datetime.time(3, 0, 0)]}, schema=pa.schema([t_field]))
        )
        assert tables.query(f"SELECT id FROM {tables.source(eqdel)} ORDER BY id") == "0\n2\n3\n5\n6", required

    # `OPTIMIZE` rewrites only the data files that position deletes apply to, so each file below gets one.
    table_name, key_prefix, table = tables.create("time_compaction")
    for k in range(3):
        table.append(rows([i for i in range(8) if i % 3 == k]))
    written_by_pyiceberg = {task.file.file_path.rsplit("/", 1)[1] for task in table.scan().plan_files()}
    write_settings = {"allow_insert_into_iceberg": 1, "allow_experimental_iceberg_compaction": 1}
    tables.instance.query(
        f"""
        CREATE TABLE {table_name} ENGINE = IcebergS3(s3, filename = '{key_prefix}/', format = Parquet,
            url = 'http://minio1:9001/{tables.cluster.minio_bucket}/')
        """
    )
    tables.instance.query(f"ALTER TABLE {table_name} DELETE WHERE id IN (0, 1, 2)", settings=write_settings)

    # The delete is committed as `v<N>.metadata.json` next to pyiceberg's `<N>-<uuid>.metadata.json`
    # files, and `OPTIMIZE` refuses a table with both schemes.
    minio = tables.cluster.minio_client
    bucket = tables.cluster.minio_bucket
    for item in minio.list_objects(bucket, f"{key_prefix}/metadata/"):
        if re.fullmatch(r"\d+-[0-9a-f-]+\.metadata\.json", item.object_name.rsplit("/", 1)[1]):
            minio.remove_object(bucket, item.object_name)
    tables.instance.query(f"OPTIMIZE TABLE {table_name}", settings=write_settings)

    # The rows below are read from the files written by `OPTIMIZE`, not from pyiceberg's.
    source = tables.source(key_prefix)
    read_files = set(tables.query(f"SELECT DISTINCT _file FROM {source}").split())
    assert read_files and read_files.isdisjoint(written_by_pyiceberg), (read_files, written_by_pyiceberg)
    assert tables.query(f"SELECT id, t FROM {source} ORDER BY id") == expected(range(3, 8), nested=False)
    assert tables.query(f"SELECT id FROM {source} WHERE t = {micros_of(1)} ORDER BY id") == "4\n7"
