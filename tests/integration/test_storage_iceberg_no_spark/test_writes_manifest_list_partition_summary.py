"""Every manifest that an INSERT or a DELETE writes holds exactly one partition tuple, so its
manifest-list entry must carry a field summary pinned to that tuple: `lower_bound == upper_bound`
per partition field. Without the summary a reader cannot skip a manifest by partition and has to
open all of them.

A summary must never be worse than absent, either: one that says `contains_null = false` and carries
no bounds states that the partition field holds no orderable value, and a spec-compliant reader
answers `ROWS_CANNOT_MATCH` and skips the whole manifest. The `partitions` list is optional, so a
tuple that cannot be described is left without one."""

import glob
import json
import math
import os
import re
import shutil
import struct
import tempfile

import avro.datafile
import avro.io
import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_last_snapshot,
    get_uuid_str,
    unescape_path,
)

TABLE_ROOT = "/var/lib/clickhouse/user_files/iceberg_data/default"


def _download_table(started_cluster, storage_type, table_name):
    path = f"{TABLE_ROOT}/{table_name}/"
    default_download_directory(started_cluster, storage_type, path, path)
    return path


def _read_avro(path):
    with open(path, "rb") as f:
        return list(avro.datafile.DataFileReader(f, avro.io.DatumReader()))


def _manifest_list_of_last_snapshot(table_path):
    snapshot_id = get_last_snapshot(table_path)
    matches = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(matches) == 1, matches
    return matches[0]


def _manifest_list_of_newest_metadata(table_path):
    """The manifest list of the newest snapshot, resolved by `(last-updated-ms, metadata version)`.
    `get_last_snapshot` compares timestamps alone, so two commits that land in the same millisecond
    leave the winner to `os.listdir` order."""
    metadata_dir = f"{table_path}/metadata"
    best_key = None
    snapshot_id = None
    for filename in os.listdir(metadata_dir):
        if not filename.endswith(".json"):
            continue
        with open(os.path.join(metadata_dir, filename)) as f:
            data = json.load(f)
        version = re.match(r"v?0*(\d+)", filename)
        key = (data.get("last-updated-ms", 0), int(version.group(1)) if version else 0)
        if best_key is None or key > best_key:
            best_key = key
            snapshot_id = data.get("current-snapshot-id")
    matches = glob.glob(f"{table_path}/metadata/snap-{snapshot_id}-*.avro")
    assert len(matches) == 1, matches
    return matches[0]


def _encode_bound(value):
    """A single-value bound as Iceberg serializes it: UTF-8 for a string, little-endian two's
    complement for an int."""
    if isinstance(value, str):
        return value.encode()
    return struct.pack("<i", value)


def _entries_of_last_snapshot(table_path, manifest_list=None):
    """Every manifest-list entry of the newest snapshot, paired with the partition tuple stored
    inside the manifest it points at. Pairing by manifest path is what makes a summary attached to
    the wrong entry visible."""
    entries = []
    for entry in _read_avro(manifest_list or _manifest_list_of_last_snapshot(table_path)):
        manifest = os.path.join(
            table_path, "metadata", os.path.basename(unescape_path(entry["manifest_path"]))
        )
        partition = {}
        for record in _read_avro(manifest):
            partition.update(record["data_file"]["partition"])
        entries.append((entry.get("content", 0), partition, entry["partitions"]))
    return entries


def _summary_tuples(summary):
    return [
        (
            field["contains_null"],
            field["contains_nan"],
            field["lower_bound"],
            field["upper_bound"],
        )
        for field in summary
    ]


def _expected_summary(partition):
    return [
        (False, None, _encode_bound(value), _encode_bound(value))
        for value in partition.values()
    ]


def _declared_partition_types(table_path):
    """The Avro type each manifest declares for its partition fields, read from the id-carrying
    `avro.schema` header the writer stores. A bound is only decodable by a reader if it is the width
    of the type declared here, so the two are asserted together."""
    declared = []
    for entry in _read_avro(_manifest_list_of_last_snapshot(table_path)):
        manifest = os.path.join(
            table_path, "metadata", os.path.basename(unescape_path(entry["manifest_path"]))
        )
        with open(manifest, "rb") as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = json.loads(dict(reader.meta)["avro.schema"])
            reader.close()
        data_file = next(f for f in schema["fields"] if f["name"] == "data_file")
        partition = next(f for f in data_file["type"]["fields"] if f["name"] == "partition")
        declared.append(
            [
                (field["name"], tuple(field["type"]) if isinstance(field["type"], list) else field["type"])
                for field in partition["type"]["fields"]
            ]
        )
    return declared


def _drop_bounds_in_manifest_list(instance, table_path, manifest_list, drop_bounds):
    """Rewrite a manifest list in the node's own filesystem so its field summaries keep
    `contains_null = false` but lose their bounds, which is what a writer without the check under
    test produced. With `upper_only` just the upper bound goes, which is the same claim to a reader
    that consults both. Returns the number of rewritten entries, which the caller asserts."""
    with open(manifest_list, "rb") as f:
        reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        schema = reader.datum_reader.writers_schema
        metadata = dict(reader.meta)
        records = list(reader)
        reader.close()

    patched = 0
    for record in records:
        for summary in record["partitions"] or []:
            assert summary["lower_bound"] is not None
            if drop_bounds == "both":
                summary["lower_bound"] = None
            summary["upper_bound"] = None
            patched += 1

    temp_dir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(temp_dir, os.path.basename(manifest_list))
        with open(local_path, "wb") as f:
            writer = avro.datafile.DataFileWriter(f, avro.io.DatumWriter(), schema)
            for key, value in metadata.items():
                if not key.startswith("avro."):
                    writer.set_meta(key, value)
            for record in records:
                writer.append(record)
            writer.close()
        instance.copy_file_to_container(local_path, manifest_list)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return patched


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    # Two partition fields of different types: the summary must list them in spec order, with a
    # string bound written as UTF-8 and an int bound as little-endian bytes.
    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(region String, part Int32, id Int32)",
        format_version,
        partition_by="(region, part)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 10, 1), ('eu', 10, 2), ('us', 11, 3);",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    assert len(entries) == 2
    assert sorted(partition["region"] for _, partition, _ in entries) == ["eu", "us"]
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_no_summary_when_unpartitioned(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_no_summary_when_unpartitioned_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32)",
        format_version,
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1), (2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    # A table without partition fields has nothing to summarize, and the field is left null rather
    # than written as an empty list.
    assert [summary for _, _, summary in entries] == [None]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary_after_delete(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_after_delete_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(region String, id Int32)",
        2,
        partition_by="(region)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 1), ('us', 2);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1;",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )

    # The DELETE writes a position-delete manifest of its own and copies the data manifests of the
    # parent snapshot: both kinds of entry must come out with bounds.
    assert sorted(content for content, _, _ in entries) == [0, 0, 1]
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "us\t2\n"


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary_bucket_transform(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_bucket_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(s String, id Int32)",
        2,
        partition_by="(icebergBucket(16, s))",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 1), ('us', 2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    table_path = _download_table(
        started_cluster_iceberg_no_spark, storage_type, TABLE_NAME
    )
    entries = _entries_of_last_snapshot(table_path)

    # The Iceberg partition field of a `bucket[N]` transform is an `int`, so its bound is the four
    # little-endian bytes of the bucket number stored in the manifest.
    assert len(entries) == 2
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)

    # Pin: the manifest declares that field as Avro `"int"` both before and after this fix, so the
    # bound's width and the declared type agree.
    assert _declared_partition_types(table_path) == [[("s", "int")], [("s", "int")]]

    # These bounds are also what ClickHouse's own manifest-list pruner reads. Without a bound it
    # substitutes infinities and never skips a bucket manifest, so this path only becomes live here.
    assert instance.query(f"SELECT id FROM {TABLE_NAME} WHERE s = 'eu'") == "1\n"
    assert instance.query(f"SELECT id FROM {TABLE_NAME} WHERE s = 'us'") == "2\n"
    assert instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE s = 'nowhere'") == "0\n"


@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_manifest_list_partition_summary_after_compaction(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_compaction_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(s String, id Int32)",
        2,
        partition_by="(icebergBucket(16, s))",
        order_by="id",
    )
    for values in ("('eu', 1), ('us', 2)", "('eu', 3), ('us', 4)"):
        instance.query(
            f"INSERT INTO {TABLE_NAME} VALUES {values};",
            settings={"allow_insert_into_iceberg": 1},
        )
    # One manifest per INSERT per bucket. Pinning this count and the manifest-list name is what
    # makes the assertions below read a list that compaction wrote: a compaction that silently does
    # nothing leaves these four entries, whose summaries satisfy the same oracle.
    before_path = _download_table(
        started_cluster_iceberg_no_spark, storage_type, TABLE_NAME
    )
    before_list = _manifest_list_of_newest_metadata(before_path)
    assert len(_entries_of_last_snapshot(before_path, before_list)) == 4

    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    table_path = _download_table(
        started_cluster_iceberg_no_spark, storage_type, TABLE_NAME
    )
    after_list = _manifest_list_of_newest_metadata(table_path)
    assert os.path.basename(after_list) != os.path.basename(before_list)
    entries = _entries_of_last_snapshot(table_path, after_list)
    assert len(entries) == 2

    # Compaction rewrites the manifest list through its own `generateManifestList` call, so a
    # rewritten entry carries the same four-byte bucket bound the insert path writes.
    for _, partition, summary in entries:
        assert summary is not None
        assert _summary_tuples(summary) == _expected_summary(partition)

    assert (
        instance.query(f"SELECT id FROM {TABLE_NAME} WHERE s = 'eu' ORDER BY id")
        == "1\n3\n"
    )
    assert instance.query(f"SELECT count() FROM {TABLE_NAME}") == "4\n"


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_no_summary_when_value_has_no_bound(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_no_summary_when_value_has_no_bound_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(f Float64, id Int32)",
        2,
        partition_by="(f)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1.5, 1), (nan, 2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, storage_type, TABLE_NAME)
    )
    assert len(entries) == 2
    summaries = {
        ("nan" if math.isnan(partition["f"]) else "value"): summary
        for _, partition, summary in entries
    }

    # A `Float64` bound is not serialized, so the tuple holding 1.5 gets no summary at all rather
    # than one claiming the field holds nothing.
    assert summaries["value"] is None
    # Negative control: a NaN partition value genuinely has no orderable bound, which `contains_nan`
    # states truthfully, so its list is kept. Dropping it here would mean the check fires on any
    # missing bound instead of on the contradiction.
    assert _summary_tuples(summaries["nan"]) == [(False, True, None, None)]


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_manifest_list_partition_summary_nullable_bucket(
    started_cluster_iceberg_no_spark, storage_type
):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_nullable_bucket_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(s Nullable(String), id Int32)",
        2,
        partition_by="(icebergBucket(16, s))",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 1), (NULL, 2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    table_path = _download_table(
        started_cluster_iceberg_no_spark, storage_type, TABLE_NAME
    )
    entries = _entries_of_last_snapshot(table_path)
    assert len(entries) == 2
    summaries = {
        ("null" if partition["s"] is None else "value"): summary
        for _, partition, summary in entries
    }

    # A nullable partition source keeps its bound, and the NULL tuple keeps a summary of its own:
    # `contains_null` alone is a complete description of it. 6 is `bucket[16]` of 'eu', which the
    # Iceberg spec fixes, so the expected bound is stated rather than read back out of the manifest.
    assert _summary_tuples(summaries["value"]) == [
        (False, None, _encode_bound(6), _encode_bound(6))
    ]
    assert _summary_tuples(summaries["null"]) == [(True, None, None, None)]

    # The nullable source keeps the Avro `["null", "int"]` union of the partition field.
    assert _declared_partition_types(table_path) == [
        [("s", ("null", "int"))],
        [("s", ("null", "int"))],
    ]


@pytest.mark.parametrize(
    "drop_bounds", ["both", "upper_only"], ids=["both_bounds", "upper_bound_only"]
)
def test_writes_manifest_list_partition_summary_carry_forward_sanitizes(
    started_cluster_iceberg_no_spark, drop_bounds
):
    """A manifest list written by a build without the check above holds the contradictory summary,
    and every later snapshot copies its entries verbatim. The copy must drop such a list instead of
    republishing it, which is the only way an existing table stops being pruned to empty.

    The sanitizer works off the copied Avro datum, so it is storage independent; this runs on local
    storage, where the file can be rewritten in place to stand in for that older writer.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_manifest_list_partition_summary_carry_forward_"
        + drop_bounds
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        "local",
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(region String, id Int32)",
        2,
        partition_by="(region)",
        order_by="id",
    )
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES ('eu', 1), ('us', 2);",
        settings={"allow_insert_into_iceberg": 1},
    )

    table_path = _download_table(started_cluster_iceberg_no_spark, "local", TABLE_NAME)
    written = _entries_of_last_snapshot(table_path)
    assert len(written) == 2
    for _, partition, summary in written:
        assert _summary_tuples(summary) == _expected_summary(partition)

    patched = _drop_bounds_in_manifest_list(
        instance, table_path, _manifest_list_of_last_snapshot(table_path), drop_bounds
    )
    assert patched == 2

    instance.query(
        f"ALTER TABLE {TABLE_NAME} DELETE WHERE id = 1;",
        settings={"allow_insert_into_iceberg": 1},
    )

    entries = _entries_of_last_snapshot(
        _download_table(started_cluster_iceberg_no_spark, "local", TABLE_NAME)
    )
    assert sorted(content for content, _, _ in entries) == [0, 0, 1]
    for content, partition, summary in entries:
        if content == 0:
            # Carried forward from the patched list: the contradiction is dropped, not propagated.
            assert summary is None
        else:
            # Written by this DELETE, so it carries a real bound.
            assert _summary_tuples(summary) == _expected_summary(partition)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "us\t2\n"
