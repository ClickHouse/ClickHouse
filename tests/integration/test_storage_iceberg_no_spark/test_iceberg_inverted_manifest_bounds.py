"""A manifest that declares a lower bound above the upper bound must not prune the data file it
describes, so a filtered read still returns every matching row. The fallback is per column: a
well-formed column in the same manifest entry must keep pruning."""

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


def _swap_one_field(lower_bounds, upper_bounds, field_id):
    """Exchange the bound stored under `field_id` between the two maps, leaving every other column
    untouched. Returns whether the id was present in both."""
    lower = next((entry for entry in lower_bounds if entry["key"] == field_id), None)
    upper = next((entry for entry in upper_bounds if entry["key"] == field_id), None)
    if lower is None or upper is None:
        return False
    lower["value"], upper["value"] = upper["value"], lower["value"]
    return True


def _swap_bounds_in_manifests(instance, table_path, only_field_id=None):
    """Exchange `data_file.lower_bounds` with `data_file.upper_bounds` in every manifest of the
    table. With `only_field_id`, exchange just that one field id and leave the other columns well
    formed. Returns the number of rewritten entries, which the caller asserts is non-zero.

    Bound maps are keyed by Iceberg field id; the ClickHouse writer assigns those sequentially from
    1 in declaration order. That assumption fails closed rather than silently: a field id that does
    not exist leaves `patched` at 0, and one naming the wrong column reddens the pruning assertion
    of the column that was supposed to stay well formed."""
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
                if only_field_id is None:
                    data_file["lower_bounds"], data_file["upper_bounds"] = (
                        data_file["upper_bounds"],
                        data_file["lower_bounds"],
                    )
                    swapped_here += 1
                elif _swap_one_field(
                    data_file["lower_bounds"], data_file["upper_bounds"], only_field_id
                ):
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

    # Control: with well-formed bounds each filter prunes the three non-matching files. Without it,
    # a fix that stopped pruning altogether would satisfy the assertions below.
    assert pruned_files(id_expr) == 3
    assert pruned_files(s_expr) == 3

    # Invert `id` alone. Its bounds now describe an empty range, so pruning on them would drop the
    # file holding matching rows and no file may be pruned; `s` is still well formed in the same
    # manifest entry and must keep pruning. Together these pin the fallback as per column rather
    # than a manifest-wide loss of min/max pruning.
    assert _swap_bounds_in_manifests(instance, table_path, only_field_id=1) > 0
    assert pruned_files(id_expr) == 0
    assert pruned_files(s_expr) == 3

    # Invert `s` as well, so no column has usable bounds. The fallback holds whichever column is
    # inverted, and neither filter may prune.
    assert _swap_bounds_in_manifests(instance, table_path, only_field_id=2) > 0
    assert pruned_files(s_expr) == 0
    assert pruned_files(id_expr) == 0
