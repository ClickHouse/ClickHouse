"""A manifest that declares a lower bound above the upper bound must not prune the data file it
describes, so a filtered read still returns every matching row."""

import os
import shutil
import tempfile

import avro.datafile
import avro.io

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    get_uuid_str,
)

NOCACHE = {
    "use_iceberg_metadata_files_cache": False,
    "use_parquet_metadata_cache": False,
}


def _swap_bounds_in_manifests(instance, table_path):
    """Swap `data_file.lower_bounds` with `data_file.upper_bounds` in every manifest of the table.
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

            swapped_here = 0
            for record in records:
                data_file = record.get("data_file")
                # Manifest-list entries carry manifest_path instead of data_file.
                if not isinstance(data_file, dict):
                    continue
                if data_file.get("lower_bounds") is None or data_file.get("upper_bounds") is None:
                    continue
                data_file["lower_bounds"], data_file["upper_bounds"] = (
                    data_file["upper_bounds"],
                    data_file["lower_bounds"],
                )
                swapped_here += 1

            if swapped_here == 0:
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
            patched += swapped_here
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
    return patched


def test_iceberg_inverted_manifest_bounds(started_cluster_iceberg_no_spark):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_iceberg_inverted_manifest_bounds_" + get_uuid_str()
    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"

    instance.query(
        f"CREATE TABLE {table_name} (id Int64, s String) ENGINE = IcebergLocal('{table_path}/', 'Parquet')",
        settings=NOCACHE,
    )
    # One data file per INSERT, with disjoint id ranges so that a filter on id can prune.
    for start in (0, 1000, 2000, 3000):
        instance.query(
            f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers({start}, 100)",
            settings=NOCACHE,
        )
    assert instance.query(f"SELECT count() FROM {table_name}", settings=NOCACHE).strip() == "400"
    instance.query(f"DROP TABLE {table_name}")

    select_expression = (
        f"SELECT id FROM icebergLocal(local, path = '{table_path}/') WHERE id < 10 ORDER BY ALL"
    )

    def pruned_files():
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

    # Control: with well-formed bounds the three non-matching files are pruned. Without it, a fix
    # that stopped pruning altogether would pass the assertion below.
    assert pruned_files() == 3

    assert _swap_bounds_in_manifests(instance, table_path) > 0

    # Inverted bounds describe an empty range. Pruning on it would drop files holding matching rows,
    # so no file may be pruned and the result must equal the unpruned read.
    assert pruned_files() == 0
