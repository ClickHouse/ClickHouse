import pytest

from helpers.export_partition_helpers import (
    EXTRA_SOURCE_COLUMN_MODES,
    make_iceberg_s3,
    make_source,
    unique_suffix,
    wait_for_export_status,
)

CLUSTER_INSTANCES = ["replica1"]

# Destination schema matching for `EXPORT PARTITION` into Iceberg: column counts, by-name versus
# by-position matching, the extra-source-column opt-in, and value-preserving versus lossy casts.


def test_export_partition_column_count_mismatch_source_more_is_rejected(cluster, source_engine):
    """
    Source has 3 columns (id, year, extra), destination has 2 (id, year).
    The ALTER must be rejected synchronously with NUMBER_OF_COLUMNS_DOESNT_MATCH,
    nothing must be scheduled in system.partition_exports, and the
    Iceberg table must remain empty.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_more_{uid}"
    iceberg_table = f"iceberg_count_more_{uid}"

    make_source(node, mt_table, "id Int64, year Int32, extra String", "year",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH for source>dest column count, "
        f"got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


def test_export_partition_column_count_mismatch_source_fewer_is_rejected(cluster, source_engine):
    """
    Source has 2 columns (id, year), destination has 3 (id, year, extra).
    Same expected synchronous rejection as the source>dest case.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_{uid}"
    iceberg_table = f"iceberg_count_fewer_{uid}"

    make_source(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int32, extra String",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH for source<dest column count, "
        f"got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


@pytest.mark.parametrize("schema_match_mode", EXTRA_SOURCE_COLUMN_MODES)
def test_export_partition_source_more_columns_allowed_with_ignore_extra_setting(cluster, schema_match_mode, source_engine):
    """
    Source has 3 columns (id, year, extra), destination has 2 (id, year).
    With `export_merge_tree_part_schema_match_mode` set to either `POSITION` or
    `NAME` and `export_merge_tree_part_ignore_extra_source_columns = 1`, the export must succeed: the
    trailing `extra` source column is dropped and only `id`/`year` land in the destination.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_{uid}"
    iceberg_table = f"iceberg_ignore_extra_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32, extra String",
             partition_by="year", replica_name="replica1", engine=source_engine)
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar'), (3, 2020, 'baz')"
    )

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int64, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
        },
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH without the setting, got: {error!r}"
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": "POSITION",
        },
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH with schema_match_mode='POSITION', got: {error!r}"
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": schema_match_mode,
            "export_merge_tree_part_ignore_extra_source_columns": 1,
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


@pytest.mark.parametrize(
    "schema_match_mode,expected_error",
    [
        pytest.param("POSITION", "NUMBER_OF_COLUMNS_DOESNT_MATCH", id="by-position"),
        pytest.param("NAME", "NUMBER_OF_COLUMNS_DOESNT_MATCH", id="by-name"),
    ],
)
def test_export_partition_column_count_mismatch_source_fewer_still_rejected_with_ignore_extra_setting(
    cluster, schema_match_mode, expected_error, source_engine
):
    """
    Setting `export_merge_tree_part_ignore_extra_source_columns = 1` never relaxes the source-has-fewer-columns
    direction, in either `POSITION` or `NAME` mode. Source has 2 columns
    (id, year), destination has 3 (id, year, extra): the destination cannot be filled from the
    source, so this must still be rejected synchronously even with the relaxed setting.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_fewer_{uid}"
    iceberg_table = f"iceberg_ignore_extra_fewer_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int64, year Int32, extra String",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": schema_match_mode,
            "export_merge_tree_part_ignore_extra_source_columns": 1,
        },
    )
    assert expected_error in error, (
        f"Expected {expected_error} for source<dest column count with {schema_match_mode}, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after rejected export, got {count}"
    )


def test_export_partition_column_count_mismatch_source_fewer_reports_column_count_error_despite_name_mismatch(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_name_{uid}"
    iceberg_table = f"iceberg_count_fewer_name_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="renamed_id Int64, year Int32, extra Int32",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH to take precedence over the 'id'/'renamed_id' "
        f"name mismatch, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_column_count_mismatch_source_fewer_reports_column_count_error_despite_type_mismatch(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_count_fewer_type_{uid}"
    iceberg_table = f"iceberg_count_fewer_type_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id String, year Int32, extra Int32",
                    partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "NUMBER_OF_COLUMNS_DOESNT_MATCH" in error, (
        f"Expected NUMBER_OF_COLUMNS_DOESNT_MATCH to take precedence over the 'id' "
        f"type mismatch, got: {error!r}"
    )
    assert "INCOMPATIBLE_COLUMNS" not in error, (
        f"Column-count mismatch must be reported before any per-column cast check, got: {error!r}"
    )

    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


def test_export_partition_key_arity_mismatch_is_rejected(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_pkey_arity_{uid}"
    iceberg_table = f"iceberg_pkey_arity_{uid}"

    make_source(node=node, name=mt_table, columns="id Int32, year Int32", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="(year, id)")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition key arity mismatch, got: {error!r}"
    )
    assert "partition" in error.lower(), (
        f"Expected error to mention the partition scheme mismatch, got: {error!r}"
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


@pytest.mark.parametrize("schema_match_mode", EXTRA_SOURCE_COLUMN_MODES)
def test_export_partition_ignore_extra_setting_prefix_contains_different_type_rejected_without_lossy_cast(
    cluster, schema_match_mode, source_engine
):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_lossy_reject_{uid}"
    iceberg_table = f"iceberg_ignore_extra_lossy_reject_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": schema_match_mode,
            "export_merge_tree_part_ignore_extra_source_columns": 1,
        },
    )
    assert "INCOMPATIBLE_COLUMNS" in error, (
        f"Expected INCOMPATIBLE_COLUMNS for the lossy cast on the kept 'id' column, got: {error!r}"
    )
    assert "lossy cast" in error, f"Expected 'lossy cast' in error, got: {error!r}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in Iceberg table after rejected export, got {count}"


@pytest.mark.parametrize("schema_match_mode", EXTRA_SOURCE_COLUMN_MODES)
def test_export_partition_ignore_extra_setting_prefix_contains_different_type_succeeds_with_lossy_cast(
    cluster, schema_match_mode, source_engine
):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_lossy_ok_{uid}"
    iceberg_table = f"iceberg_ignore_extra_lossy_ok_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": schema_match_mode,
            "export_merge_tree_part_ignore_extra_source_columns": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    result = node.query(
        f"SELECT id, toTypeName(id), year FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt32\t2020\n2\tInt32\t2020", f"Unexpected data:\n{result}"


def test_export_partition_ignore_extra_setting_prefix_contains_different_name(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_renamed_{uid}"
    iceberg_table = f"iceberg_ignore_extra_renamed_{uid}"

    make_source(node=node, name=mt_table, columns="id Int64, year Int32, extra String", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'foo'), (2, 2020, 'bar'), (3, 2020, 'baz')")

    make_iceberg_s3(node=node, name=iceberg_table, columns="renamed_id Int64, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": "POSITION",
            "export_merge_tree_part_ignore_extra_source_columns": 1,
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    result = node.query(
        f"SELECT renamed_id, year FROM {iceberg_table} ORDER BY renamed_id"
    ).strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


@pytest.mark.parametrize("schema_match_mode", EXTRA_SOURCE_COLUMN_MODES)
def test_export_partition_matches_columns_when_column_counts_are_equal(cluster, schema_match_mode, source_engine):
    """Source and destination declare the same 2 columns, in the same order and with the same
    names, so `POSITION` and `NAME` must agree and both succeed identically -
    there is no unmatched source column for `export_merge_tree_part_ignore_extra_source_columns` to affect."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_ignore_extra_noop_{uid}"
    iceberg_table = f"iceberg_ignore_extra_noop_{uid}"

    make_source(node=node, name=mt_table, columns="id Int32, year Int32", partition_by="year",
              replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")

    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_schema_match_mode": schema_match_mode,
        },
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", f"Unexpected data:\n{result}"


def test_export_partition_column_count_mismatch_into_table_with_existing_data(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_seed_table = f"mt_existing_data_seed_{uid}"
    mt_table = f"mt_existing_data_{uid}"
    iceberg_table = f"iceberg_existing_data_{uid}"

    ignore_extra_settings = {
        "allow_insert_into_iceberg": 1,
        "export_merge_tree_part_schema_match_mode": "POSITION",
        "export_merge_tree_part_ignore_extra_source_columns": 1,
    }

    make_source(node=node, name=mt_seed_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1", engine=source_engine)
    make_source(node=node, name=mt_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(f"INSERT INTO {mt_seed_table} VALUES (100, 2020, 'x'), (101, 2021, 'y')")
    node.query(
        f"ALTER TABLE {mt_seed_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_seed_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")
    node.query(
        f"ALTER TABLE {mt_seed_table} EXPORT PARTITION ID '2021' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_seed_table, dest_table=iceberg_table,
                            partition_id="2021", expected_status="COMPLETED")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'a'), (2, 2020, 'b')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 4, f"Expected 4 rows (2 pre-existing + 2 exported), got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n100\t2020\n101\t2021", (
        f"Unexpected data after exporting into a table with pre-existing rows:\n{result}"
    )


def test_export_partition_column_count_mismatch_into_partition_that_already_has_data(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_repeat_partition_{uid}"
    iceberg_table = f"iceberg_repeat_partition_{uid}"

    ignore_extra_settings = {
        "allow_insert_into_iceberg": 1,
        "export_merge_tree_part_schema_match_mode": "POSITION",
        "export_merge_tree_part_ignore_extra_source_columns": 1,
    }

    make_source(node=node, name=mt_table, columns="id Int32, year Int32, extra String",
              partition_by="year", replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node=node, name=iceberg_table, columns="id Int32, year Int32", partition_by="year")

    node.query(f"SYSTEM STOP MERGES {mt_table}")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'a'), (2, 2020, 'b')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings=ignore_extra_settings,
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count_after_first = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_after_first == 2, (
        f"Expected 2 rows after first export, got {count_after_first}"
    )

    node.query(f"INSERT INTO {mt_table} VALUES (3, 2020, 'c'), (4, 2020, 'd')")
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={**ignore_extra_settings, "export_merge_tree_partition_force_export": 1},
    )
    wait_for_export_status(node=node, source_table=mt_table, dest_table=iceberg_table,
                            partition_id="2020", expected_status="COMPLETED")

    count_after_second = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count_after_second == 6, (
        f"Expected 6 rows (2 original + 2 duplicated by the forced re-export + 2 new) "
        f"after re-exporting an already-populated partition, got {count_after_second}"
    )

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n1\t2020\n2\t2020\n2\t2020\n3\t2020\n4\t2020", (
        f"Unexpected data after two exports of the same partition:\n{result}"
    )


def test_export_partition_with_renamed_destination_column(cluster, source_engine):
    """
    Source has column `id`, destination has the same shape but the column is
    named `renamed_id`.  Positional matching must accept the export and the
    data must land in the destination under the new name.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_renamed_{uid}"
    iceberg_table = f"iceberg_renamed_{uid}"

    make_source(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020)")

    make_iceberg_s3(node, iceberg_table, "renamed_id Int64, year Int32",
                    partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT renamed_id, year FROM {iceberg_table} ORDER BY renamed_id"
    ).strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data under renamed column:\n{result}"
    )


def test_export_partition_with_castable_widening(cluster, source_engine):
    """A lossless widening of both a data column (id Int32 -> Int64) and the
    partition column (year Int32 -> Int64) round-trips."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_widen_{uid}"
    iceberg_table = f"iceberg_widen_{uid}"

    make_source(node, mt_table, "id Int32, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int64", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT id, toTypeName(id), year, toTypeName(year) FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt64\t2020\tInt64\n2\tInt64\t2020\tInt64", (
        f"Unexpected widened data:\n{result}"
    )


def test_export_partition_with_castable_narrowing_values_fit(cluster, source_engine):
    """A lossy narrowing (id Int64 -> Int32) succeeds once the user opts in via
    export_merge_tree_part_allow_lossy_cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_narrow_fit_{uid}"
    iceberg_table = f"iceberg_narrow_fit_{uid}"

    make_source(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in Iceberg table after export, got {count}"

    result = node.query(
        f"SELECT id, toTypeName(id), year FROM {iceberg_table} ORDER BY id"
    ).strip()
    assert result == "1\tInt32\t2020\n2\tInt32\t2020", (
        f"Unexpected narrowed data:\n{result}"
    )


def test_export_partition_lossy_cast_rejected_without_optin(cluster, source_engine):
    """A lossy narrowing (id Int64 -> Int32) is rejected synchronously with
    INCOMPATIBLE_COLUMNS unless export_merge_tree_part_allow_lossy_cast is set."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_reject_{uid}"
    iceberg_table = f"iceberg_lossy_reject_{uid}"

    make_source(node, mt_table, "id Int64, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} "
        f"SETTINGS allow_insert_into_iceberg = 1"
    )
    assert "INCOMPATIBLE_COLUMNS" in error, f"Expected INCOMPATIBLE_COLUMNS, got: {error!r}"
    assert "lossy cast" in error, f"Expected 'lossy cast' in error, got: {error!r}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected no rows after a rejected export, got {count}"


def test_export_partition_runtime_cast_failure_propagates_async(cluster, source_engine):
    """A String value that cannot be parsed as the destination Int32 passes the
    synchronous lossy-cast gate (with export_merge_tree_part_allow_lossy_cast = 1) but
    fails at runtime in the async worker with CANNOT_PARSE_TEXT. That is a deterministic
    value-conversion error on the part's immutable data — retrying the same part can
    never succeed — so it is classified as non-retryable and fails the whole task fast,
    without waiting for the absolute task timeout, leaving Iceberg empty.

    The task timeout is left at its large default, so reaching FAILED quickly proves the
    transition is driven by error classification rather than by a timeout.

    (Integer overflow is not used because the internal cast uses CastType::nonAccurate,
    which wraps rather than throwing.)
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_runtime_cast_fail_{uid}"
    iceberg_table = f"iceberg_runtime_cast_fail_{uid}"

    make_source(node, mt_table, "id String, year Int32", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES ('not a number', 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year Int32", partition_by="year")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table} "
        f"SETTINGS allow_insert_into_iceberg = 1, export_merge_tree_part_allow_lossy_cast = 1"
    )

    # The runtime parse error (CANNOT_PARSE_TEXT) is non-retryable, so the task fails fast.
    # No short timeout is set; FAILED within this window can only come from the
    # non-retryable classification, not from the (default, ~1 day) task timeout.
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "FAILED", timeout=60)

    exception_count = int(node.query(
        f"SELECT any(exception_count) FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{iceberg_table}' "
        f"  AND partition_id = '2020'"
    ).strip())
    assert exception_count > 0, (
        "Expected non-zero exception_count after a failed runtime cast"
    )

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, (
        f"Expected 0 rows in Iceberg table after failed export, got {count}"
    )


def test_export_partition_all_iceberg_types(cluster, source_engine):
    """Every getIcebergType-supported type round-trips through an EXPORT PARTITION:
    scalars use narrower source types (explicit lossless widening CASTs), plus
    Array/Map/Tuple nested columns."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_all_types_{uid}"
    iceberg_table = f"iceberg_all_types_{uid}"

    # Scalar source types are strictly narrower than the destination; the export inserts
    # a positional widening CAST per column (Int8->Int16, UInt32->UInt64, ...). Nested
    # columns keep the same type on both sides.
    source_columns = (
        "i16 Int8, u16 UInt8, u32 UInt16, u64 UInt32, "
        "id Int16, big Int32, f32 Float32, f64 Float64, "
        "d Date, d32 Date32, dt DateTime, dt64 DateTime64(6), "
        "s String, uid UUID, "
        "arr Array(Int32), m Map(String, Int64), tup Tuple(a Int32, b String), "
        "year Int32"
    )
    dest_columns = (
        "i16 Int16, u16 UInt16, u32 UInt32, u64 UInt64, "
        "id Int32, big Int64, f32 Float32, f64 Float64, "
        "d Date, d32 Date32, dt DateTime, dt64 DateTime64(6), "
        "s String, uid UUID, "
        "arr Array(Int32), m Map(String, Int64), tup Tuple(a Int32, b String), "
        "year Int32"
    )

    make_source(node, mt_table, source_columns, "year", replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node, iceberg_table, dest_columns, partition_by="year")

    node.query(
        f"""
        INSERT INTO {mt_table}
            (i16, u16, u32, u64, id, big, f32, f64, d, d32, dt, dt64, s, uid, arr, m, tup, year)
        VALUES (
            -100, 200, 50000, 4000000000,
            12345, 1000000000, 3.14, 2.718281828459045,
            '2024-01-15', '2024-01-15', '2024-01-15 12:30:45', '2024-01-15 12:30:45.123456',
            'hello iceberg', '550e8400-e29b-41d4-a716-446655440000',
            [1, 2, 3], {{'a': 10, 'b': 20}}, (7, 'seven'), 2024
        )
        """
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2024' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2024", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 1, f"Expected 1 row in Iceberg table, got {count}"

    result = node.query(
        f"""
        SELECT
            i16, u16, u32, u64, id, big,
            toString(d), toString(d32), toString(dt),
            s, toString(uid),
            arr, m['a'], m['b'], tup.a, tup.b, year
        FROM {iceberg_table}
        """
    ).strip()
    expected = "\t".join([
        "-100", "200", "50000", "4000000000",
        "12345", "1000000000",
        "2024-01-15", "2024-01-15", "2024-01-15 12:30:45.000000",
        "hello iceberg", "550e8400-e29b-41d4-a716-446655440000",
        "[1,2,3]", "10", "20", "7", "seven", "2024",
    ])
    assert result == expected, f"Unexpected round-trip data:\n{result!r}\nexpected:\n{expected!r}"

    # Floats compared with a tolerance to avoid formatting flakiness.
    floats_ok = node.query(
        f"SELECT abs(f32 - 3.14) < 1e-4 AND abs(f64 - 2.718281828459045) < 1e-12 FROM {iceberg_table}"
    ).strip()
    assert floats_ok == "1", f"Float round-trip outside tolerance: {floats_ok!r}"

    # DateTime64 sub-second component: assert the date part is preserved (exact format varies).
    ts_result = node.query(f"SELECT dt64 FROM {iceberg_table}").strip()
    assert "2024-01-15" in ts_result, f"DateTime64 date component missing: {ts_result!r}"


def test_export_partition_all_iceberg_types_lossy(cluster, source_engine):
    """Lossy narrowing casts across types succeed with the opt-in flag: values that
    fit round-trip, Float64 -> Float32 loses precision, and Nullable columns carry
    both NULL and non-NULL (the latter via a lossy Nullable(Int64) -> Nullable(Int32))."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_types_{uid}"
    iceberg_table = f"iceberg_lossy_types_{uid}"

    # Each source column is wider than the destination, so the export inserts a lossy
    # narrowing CAST (allowed only because export_merge_tree_part_allow_lossy_cast=1).
    # Int8/UInt8 are not Iceberg-representable, so the narrowest integer dest is Int16.
    source_columns = (
        "big Int64, ubig UInt64, mid Int32, "
        "f Float64, dt DateTime64(6), d Date32, "
        "opt_s Nullable(String), opt_i Nullable(Int64), year Int32"
    )
    dest_columns = (
        "big Int32, ubig UInt32, mid Int16, "
        "f Float32, dt DateTime, d Date, "
        "opt_s Nullable(String), opt_i Nullable(Int32), year Int32"
    )

    make_source(node, mt_table, source_columns, "year", replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node, iceberg_table, dest_columns, partition_by="year")

    # Values chosen to fit the destination types (the async cast wraps on overflow
    # rather than throwing, so out-of-range values would silently corrupt instead).
    # opt_s is NULL and opt_i is set, covering both nullable paths in one row.
    node.query(
        f"""
        INSERT INTO {mt_table} (big, ubig, mid, f, dt, d, opt_s, opt_i, year)
        VALUES (
            1000000, 2000000000, 30000,
            2.718281828459045, '2024-01-15 12:30:45.123456', '2024-01-15',
            NULL, 100, 2024
        )
        """
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2024' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2024", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 1, f"Expected 1 row in Iceberg table, got {count}"

    result = node.query(
        f"SELECT big, ubig, mid, toString(d), toString(dt), opt_s, opt_i, year FROM {iceberg_table}"
    ).strip()
    expected = "\t".join([
        "1000000", "2000000000", "30000",
        "2024-01-15", "2024-01-15 12:30:45.000000", "\\N", "100", "2024",
    ])
    assert result == expected, f"Unexpected lossy round-trip data:\n{result!r}\nexpected:\n{expected!r}"

    # Float64 -> Float32 stays within Float32 precision but is no longer exact.
    f_checks = node.query(
        f"SELECT abs(f - 2.718281828459045) < 1e-6, abs(f - 2.718281828459045) > 1e-9 FROM {iceberg_table}"
    ).strip()
    assert f_checks == "1\t1", f"Expected Float32 precision loss within tolerance, got: {f_checks!r}"
