"""A position-delete manifest may declare the range of data file paths its delete file references.
A lower bound above the upper bound describes no range, so it says nothing about which data files
the delete file covers and must not skip it for any of them: the deleted rows stay deleted."""

import os
import shutil
import tempfile

import avro.datafile
import avro.io

from helpers.iceberg_utils import get_uuid_str

# Reserved field id of the `file_path` column of a position-delete file, matching
# `IcebergPositionDeleteTransform::data_file_path_column_field_id`.
DATA_FILE_PATH_FIELD_ID = 2147483546

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}


def _patch_data_file_path_bounds(instance, table_path, mutate):
    """Apply `mutate` to the `lower_bounds` and `upper_bounds` entries under the reserved `file_path`
    field id in every manifest entry of the table that carries one, and rewrite the manifests
    changed. Only a position-delete entry has a bound under that id, a data file entry keying its
    bounds by the table's own column ids, so this needs no filter on the entry's content type to
    leave the data manifests alone.

    Returns the number of rewritten entries, which the caller asserts is non-zero."""
    patched = 0
    temp_dir = tempfile.mkdtemp()
    try:
        for remote_path in instance.get_files_list_in_container(f"{table_path}/metadata"):
            if not remote_path.endswith(".avro"):
                continue

            local_path = os.path.join(temp_dir, os.path.basename(remote_path))
            instance.copy_file_from_container(remote_path, local_path)

            with open(local_path, "rb") as f:
                reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
                schema = reader.datum_reader.writers_schema
                metadata = dict(reader.meta)
                records = list(reader)
                reader.close()

            patched_here = 0
            for record in records:
                data_file = record.get("data_file")
                # Manifest-list entries carry `manifest_path` instead of `data_file`.
                if not isinstance(data_file, dict):
                    continue
                lower_bounds = data_file.get("lower_bounds")
                upper_bounds = data_file.get("upper_bounds")
                if lower_bounds is None or upper_bounds is None:
                    continue
                lower = next(
                    (e for e in lower_bounds if e["key"] == DATA_FILE_PATH_FIELD_ID), None
                )
                upper = next(
                    (e for e in upper_bounds if e["key"] == DATA_FILE_PATH_FIELD_ID), None
                )
                if lower is None or upper is None:
                    continue
                mutate(lower, upper)
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

            instance.copy_file_to_container(local_path, remote_path)
            patched += patched_here
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return patched


def _swap(lower, upper):
    # The bounds are the extremes of the delete file's own `file_path` column, so a delete file
    # referencing a single data file has lower == upper and exchanging them leaves the manifest
    # well formed. The fixture covers two data files with one delete file to keep the pair
    # distinct.
    assert lower["value"] != upper["value"]
    lower["value"], upper["value"] = upper["value"], lower["value"]


def _set_above_every_path(lower, upper):
    # An inverted pair whose two values sort above every data file path. Reordering it, rather
    # than ignoring it, yields a range that contains no data file at all, so the delete file
    # would go on being skipped everywhere.
    above = max(lower["value"], upper["value"]) + b"~"
    assert above > lower["value"] and above > upper["value"]
    lower["value"], upper["value"] = above + b"~", above


def test_iceberg_inverted_delete_bounds(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_inverted_delete_bounds_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    instance.query(
        f"CREATE TABLE {table_name} (a Int64, b String) "
        f"ENGINE = IcebergLocal('{table_path}/', 'Parquet')",
        settings=NOCACHE,
    )
    write = {"allow_insert_into_iceberg": 1, **NOCACHE}
    # Two inserts make two data files, and one delete touching both makes a single delete file whose
    # `file_path` bounds are the two data file paths.
    for lowest in (0, 10):
        instance.query(
            f"INSERT INTO {table_name} SELECT number, char(number + ascii('a')) "
            f"FROM numbers({lowest}, 10)",
            settings=write,
        )
    instance.query(f"ALTER TABLE {table_name} DELETE WHERE a % 2 = 0", settings=write)
    instance.query(f"DROP TABLE {table_name}")

    select = f"SELECT a FROM icebergLocal(local, path = '{table_path}/') ORDER BY ALL"
    survivors = "".join(f"{a}\n" for a in range(1, 20, 2))

    def read():
        """Return the rows, and how many (delete file, data file) pairs the bounds skipped and kept."""
        query_id = f"{table_name}-{get_uuid_str()}"
        rows = instance.query(select, query_id=query_id, settings=NOCACHE)
        instance.query("SYSTEM FLUSH LOGS")

        def profile_event(name):
            return int(
                instance.query(
                    f"SELECT ProfileEvents['{name}'] FROM system.query_log "
                    f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
                )
            )

        return (
            rows,
            profile_event("IcebergMinMaxPrunedDeleteFiles"),
            profile_event("IcebergMinMaxNonPrunedDeleteFiles"),
        )

    # Control: with well-formed bounds the delete file is kept for both data files and no pair is
    # skipped. That pins the fixture the arms below need: no `referenced_data_file` (with one, both
    # bounds would come from that single path and one of the two pairs would be skipped), bounds
    # spanning both data file paths, and the same tuple the arms assert, which makes each of them a
    # comparison against this run rather than against a remembered number.
    assert read() == (survivors, 0, 2)

    # Inverted, the bounds describe an empty range of paths, so skipping on them would drop the
    # delete file for every data file and return the rows it deletes.
    assert _patch_data_file_path_bounds(instance, table_path, _swap) > 0
    assert read() == (survivors, 0, 2)

    assert _patch_data_file_path_bounds(instance, table_path, _set_above_every_path) > 0
    assert read() == (survivors, 0, 2)
