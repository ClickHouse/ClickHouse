import uuid

import pytest

from helpers.export_partition_helpers import (
    REJECTED_PARTITION_EXPORT_CASES,
    first_partition_id,
    skip_if_remote_database_disk_enabled,
    wait_for_export_status,
)

from .common import source_engine_clause

CLUSTER_INSTANCES = ["replica1"]

# Schedule-time validation of `EXPORT PARTITION` into a plain object-storage destination: the
# partition-key compatibility gate and the hive partition-value rendering. Rejections are
# synchronous, so these tests are cheap and touch Keeper only to create the source table.


def test_export_partition_partition_column_castable_type_mismatch(cluster, source_engine):
    """A lossy partition-column cast (year String -> UInt16) is rejected synchronously
    when export_merge_tree_part_allow_lossy_cast is off, scheduling nothing."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"pkey_cast_mismatch_partition_mt_{postfix}"
    s3_table = f"pkey_cast_mismatch_partition_s3_{postfix}"

    # Source: year String; destination: year UInt16. PARTITION BY year on
    # both sides — same AST text — to defeat the AST equivalence check.
    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year String) "
        f"ENGINE = {source_engine_clause(source_engine, mt_table)} "
        f"PARTITION BY year "
        f"ORDER BY tuple()"
    )
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16) "
        f"ENGINE = S3(s3_conn, filename='{s3_table}', "
        f"format=Parquet, partition_strategy='hive') "
        f"PARTITION BY year"
    )

    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2020'), (2, '2020'), (3, '2020')"
    )

    # With a String partition column the partition_id is the SipHash of the
    # value rather than the textual representation — look it up so we can
    # reference the partition explicitly in EXPORT PARTITION ID and in
    # subsequent system.partition_exports queries.
    partition_id = node.query(
        f"SELECT partition_id FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{mt_table}' "
        f"  AND active "
        f"ORDER BY name LIMIT 1"
    ).strip()
    assert partition_id, (
        "Expected one active part on the source table after INSERT; "
        "system.parts returned nothing."
    )

    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' "
        f"TO TABLE {s3_table}"
    )
    assert "INCOMPATIBLE_COLUMNS" in error, (
        f"Expected INCOMPATIBLE_COLUMNS for a lossy partition-column cast, "
        f"got: {error!r}"
    )
    assert "requires a lossy cast" in error and "'year'" in error, (
        f"Expected the error message to report the lossy cast on column "
        f"'year', got: {error!r}"
    )

    # Nothing scheduled: no row in system.partition_exports.
    rows_in_system_view = node.query(
        f"SELECT count() FROM system.partition_exports "
        f"WHERE source_table = '{mt_table}' "
        f"  AND destination_table = '{s3_table}' "
        f"  AND partition_id = '{partition_id}'"
    ).strip()
    assert rows_in_system_view == "0", (
        f"Expected no row in system.partition_exports after a "
        f"synchronously-rejected export, got {rows_in_system_view}."
    )

    # Nothing written: no parquet file under any year=*/ partition prefix.
    files_in_s3 = node.query(
        f"SELECT count() FROM s3(s3_conn, "
        f"filename='{s3_table}/year=*/*.parquet', format='One')"
    ).strip()
    assert files_in_s3 == "0", (
        f"Expected no Parquet files in S3 after a synchronously-rejected "
        f"export, found {files_in_s3}."
    )


# ---- Partition-key compatibility gate (unified with the Iceberg gate) --------------------------
#
# Plain (hive) object storage writes every row of a part to the single directory computed from the
# destination PARTITION BY, so each source partition must map to exactly one destination partition.
# The gate accepts equivalent or finer source keys (e.g. a source that adds partition columns on top
# of the destination's) and rejects source partitions that would span several destination partitions
# or that do not cover the destination partition column. Hive destinations partition by bare columns
# only, so these cases exercise the column-subset and single-value paths.


def _run_subset_accept(node, source_key, engine):
    """Export a source partitioned by *source_key* (a superset of the destination key ``year``) into a
    hive destination partitioned by ``year``, then verify the full dataset, the hive directory layout,
    and a round-trip back into MergeTree."""
    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"subset_mt_{uid}"
    s3_table = f"subset_s3_{uid}"
    roundtrip = f"subset_roundtrip_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = {source_engine_clause(engine, mt_table)}"
        f" PARTITION BY {source_key} ORDER BY tuple()"
    )
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, 2020, 'US'), (2, 2020, 'FR'), (3, 2021, 'US')"
    )
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY year"
    )

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts"
        f" WHERE database = currentDatabase() AND table = '{mt_table}' AND active"
    ).strip().split("\n")
    assert len(partition_ids) == 3, f"expected 3 source partitions, got {partition_ids}"

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    for pid in partition_ids:
        wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    src = node.query(f"SELECT id, year, country FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, year, country FROM {s3_table} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    # The destination partitions by year only: rows land in the year=<value> hive directory.
    rows_2020 = node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/year=2020/*.parquet', format='Parquet')"
    ).strip()
    rows_2021 = node.query(
        f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/year=2021/*.parquet', format='Parquet')"
    ).strip()
    assert rows_2020 == "2", f"expected 2 rows under year=2020, got {rows_2020}"
    assert rows_2021 == "1", f"expected 1 row under year=2021, got {rows_2021}"

    node.query(
        f"CREATE TABLE {roundtrip} (id UInt64, year UInt16, country String)"
        f" ENGINE = {source_engine_clause(engine, roundtrip)}"
        f" PARTITION BY {source_key} ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {roundtrip} SELECT * FROM {s3_table}")
    rt = node.query(f"SELECT id, year, country FROM {roundtrip} ORDER BY id")
    assert rt == src, f"round-trip rows differ from source:\nsrc={src!r}\nrt={rt!r}"


def test_export_partition_multicolumn_subset_accepted(cluster, source_engine):
    """Source partitions by (year, country); destination by year only - a coarser key that is covered
    by the source key, so every source partition has a single year and maps to exactly one destination
    partition. Accepted (this was rejected as a partition-key mismatch before the plain gate was
    unified with the Iceberg one)."""
    node = cluster.instances["replica1"]
    _run_subset_accept(node, "(year, country)", source_engine)


def test_export_partition_subset_reversed_order_accepted(cluster, source_engine):
    """The subset match is order-independent: a source keyed by (country, year) still covers a
    destination keyed by year."""
    node = cluster.instances["replica1"]
    _run_subset_accept(node, "(country, year)", source_engine)


def test_export_partition_coarser_source_rejected(cluster, source_engine):
    """Source partitions monthly (toYYYYMM(dt)); destination by the raw date. A single source part
    holding two different days would map to two destination partitions, so the gate rejects the
    export synchronously with BAD_ARGUMENTS and schedules nothing."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"coarser_mt_{uid}"
    s3_table = f"coarser_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, dt Date)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY toYYYYMM(dt) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, '2024-03-05'), (2, '2024-03-20')")
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, dt Date)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY dt"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"

    scheduled = node.query(
        f"SELECT count() FROM system.partition_exports"
        f" WHERE source_table = '{mt_table}' AND destination_table = '{s3_table}'"
    ).strip()
    assert scheduled == "0", f"expected nothing scheduled after a synchronous reject, got {scheduled}"


def test_export_partition_dest_column_not_in_source_key_rejected(cluster, source_engine):
    """Destination partitions by a column that is not part of the source partition key; the gate
    rejects the export synchronously with BAD_ARGUMENTS naming the uncovered column."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"nocover_mt_{uid}"
    s3_table = f"nocover_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY year ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020, 'US'), (2, 2020, 'FR')")
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16, country String)"
        f" ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')"
        f" PARTITION BY country"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"
    assert "country" in error, f"expected the error to name column 'country', got: {error!r}"


def test_export_partition_column_timezone_rendered_in_destination_zone(cluster, source_engine):
    """A hive partition value lives as text in the object path and is read back in the destination
    column's time zone, so the export has to spell it the way the destination would. Spelling it in the
    source's zone names a different instant and the row reads back shifted by the offset between the
    two zones. INSERT SELECT into an identical table is the reference behavior."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"tz_mt_{uid}"
    s3_export = f"tz_export_s3_{uid}"
    s3_insert = f"tz_insert_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, ts DateTime('UTC'))"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY toDate(ts) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 15:00:00')")
    for table in (s3_export, s3_insert):
        node.query(
            f"CREATE TABLE {table} (id UInt64, ts DateTime('Asia/Tokyo'))"
            f" ENGINE = S3(s3_conn, filename='{table}', format=Parquet, partition_strategy='hive')"
            f" PARTITION BY ts"
        )

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_export}")
    wait_for_export_status(node, mt_table, s3_export, pid, "COMPLETED", timeout=90)

    node.query(f"INSERT INTO {s3_insert} SELECT * FROM {mt_table}")

    source_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {mt_table}").strip()
    exported_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {s3_export}").strip()
    inserted_instant = node.query(f"SELECT toUnixTimestamp(ts) FROM {s3_insert}").strip()
    assert exported_instant == source_instant, (
        f"the exported row moved in time: source {source_instant}, destination {exported_instant}"
    )
    assert inserted_instant == source_instant, (
        f"INSERT SELECT must not move it either: source {source_instant},"
        f" destination {inserted_instant}"
    )

    # 2024-03-05 15:00:00 UTC is 2024-03-06 00:00:00 in Tokyo.
    exported_directory = node.query(
        f"SELECT DISTINCT extract(_path, 'ts=[^/]*') FROM {s3_export}"
    ).strip()
    inserted_directory = node.query(
        f"SELECT DISTINCT extract(_path, 'ts=[^/]*') FROM {s3_insert}"
    ).strip()
    assert exported_directory == "ts=2024-03-06 00:00:00", (
        f"unexpected hive directory: {exported_directory!r}"
    )
    assert inserted_directory == exported_directory, (
        f"export and INSERT SELECT disagree on the partition directory:"
        f" {exported_directory!r} vs {inserted_directory!r}"
    )


def create_wildcard_destination(node, table, columns, partition_key):
    """A wildcard destination, the only partition strategy that accepts an expression as its
    partition key: the hive strategy allows storage columns only."""
    node.query(
        f"CREATE TABLE {table} ({columns})"
        f" ENGINE = S3(s3_conn, filename='{table}/{{_partition_id}}/{{_file}}.parquet',"
        f" format=Parquet, partition_strategy='wildcard')"
        f" PARTITION BY {partition_key}"
    )


def test_export_partition_dest_argument_order_rejected(cluster, source_engine):
    """The destination key intDiv(x, 100) has to be validated as written. This source part holds
    x in [201, 350], which covers the destination partitions 2 and 3, so the export must be rejected.
    Reading the arguments in the reverse order would validate intDiv(100, x) instead, which is 0 at
    both endpoints and would silently write both destination partitions into one directory."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"argorder_mt_{uid}"
    s3_table = f"argorder_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, x UInt64)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY intDiv(x, 1000) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 201), (2, 350)")
    create_wildcard_destination(node, s3_table, "id UInt64, x UInt64", "intDiv(x, 100)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"


def test_export_partition_dest_finer_expression_single_partition_accepted(cluster, source_engine):
    """The same shape as the rejected case, with x in [100, 150]: the whole source partition maps to
    the single destination partition 1, so it is accepted and every row lands in one directory. The
    swapped-argument reading would refuse this one, since intDiv(100, 100) != intDiv(100, 150)."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"argorder_ok_mt_{uid}"
    s3_table = f"argorder_ok_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, x UInt64)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY intDiv(x, 1000) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 100), (2, 150)")
    create_wildcard_destination(node, s3_table, "id UInt64, x UInt64", "intDiv(x, 100)")

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    # A wildcard destination cannot be read as a table, so read the objects it wrote.
    exported = f"s3(s3_conn, filename='{s3_table}/**/*.parquet', format='Parquet', structure='id UInt64, x UInt64')"
    src = node.query(f"SELECT id, x FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, x FROM {exported} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    directories = node.query(
        f"SELECT DISTINCT extract(_path, '{s3_table}/[^/]*') FROM {exported}"
    ).strip()
    assert directories == f"{s3_table}/1", f"unexpected destination directories: {directories!r}"


def test_export_partition_dest_nested_expression_accepted(cluster, source_engine):
    """A destination key that wraps the source key in a coarser transform - toYYYYMM(toDate(ts)) over
    a source keyed by toDate(ts) - is a function of the source key, so every source partition sits
    inside one destination partition whatever the data is."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"nested_mt_{uid}"
    s3_table = f"nested_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, ts DateTime)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY toDate(ts) ORDER BY tuple()"
    )
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')"
    )
    create_wildcard_destination(node, s3_table, "id UInt64, ts DateTime", "toYYYYMM(toDate(ts))")

    pid = first_partition_id(node, mt_table)
    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED", timeout=90)

    exported = f"s3(s3_conn, filename='{s3_table}/**/*.parquet', format='Parquet', structure='id UInt64, ts DateTime')"
    src = node.query(f"SELECT id, ts FROM {mt_table} ORDER BY id")
    dst = node.query(f"SELECT id, ts FROM {exported} ORDER BY id")
    assert dst == src, f"destination rows differ from source:\nsrc={src!r}\ndst={dst!r}"

    directories = node.query(
        f"SELECT DISTINCT extract(_path, '{s3_table}/[^/]*') FROM {exported}"
    ).strip()
    assert directories == f"{s3_table}/202403", (
        f"unexpected destination directories: {directories!r}"
    )


def test_export_partition_dest_term_over_two_columns_rejected(cluster, source_engine):
    """A destination expression over two columns is only single-valued when the source key pins both.
    This source pins b but only intDiv(a, 100), so a spans [10, 90] within one source partition and
    intDiv(a + b, 100) takes both 0 and 1 there. Per-column min/max cannot bound such an expression,
    so it is rejected; a source keyed by (a, b) would be accepted, since it pins both columns."""
    node = cluster.instances["replica1"]

    uid = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"twocol_mt_{uid}"
    s3_table = f"twocol_s3_{uid}"

    node.query(
        f"CREATE TABLE {mt_table} (id UInt64, a UInt64, b UInt64)"
        f" ENGINE = {source_engine_clause(source_engine, mt_table)}"
        f" PARTITION BY (intDiv(a, 100), b) ORDER BY tuple()"
    )
    node.query(f"INSERT INTO {mt_table} VALUES (1, 10, 20), (2, 90, 20)")
    create_wildcard_destination(
        node, s3_table, "id UInt64, a UInt64, b UInt64", "intDiv(a + b, 100)"
    )

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {s3_table}"
    )
    assert "BAD_ARGUMENTS" in error, f"expected BAD_ARGUMENTS, got: {error!r}"


@pytest.mark.parametrize("case", REJECTED_PARTITION_EXPORT_CASES)
def test_export_partition_partition_key_mismatch_variants_are_rejected(cluster, case, source_engine):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"rejected_mt_table_{postfix}"
    s3_table = f"rejected_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} ({case.src_columns})
        ENGINE = {source_engine_clause(source_engine, mt_table)}
        PARTITION BY {case.src_partition_by}
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} ({case.dst_columns})
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY {case.dst_partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES {case.insert_values}")

    partition_id = node.query(
        f"SELECT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    error = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' TO TABLE {s3_table}")
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error}"
    for substring in case.error_substrings:
        assert substring in error, f"Expected {substring!r} in error, got: {error}"

    error_all = node.query_and_get_error(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")
    assert "BAD_ARGUMENTS" in error_all, f"Expected BAD_ARGUMENTS, got: {error_all}"

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 0, f"Expected 0 rows in destination after rejected export, got {count}"


@pytest.mark.parametrize(
    "dst_partition_by",
    ["(a, b, c)", "(c, b, a)", "(a, b)"],
    ids=["same", "reordered", "coarser"],
)
def test_export_partition_multi_column_partition_key_success(cluster, dst_partition_by, source_engine):
    """The source key pins every column the destination partitions by, so the destination may
    also name them in another order or leave some out: each destination expression is still
    single-valued over a source partition."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_pkey_ok_mt_table_{postfix}"
    s3_table = f"multi_pkey_ok_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = {source_engine_clause(source_engine, mt_table)}
        PARTITION BY (a, b, c)
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY {dst_partition_by}
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (1, 2, 3, 'y')")

    partition_id = node.query(
        f"SELECT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY name LIMIT 1"
    ).strip()

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{partition_id}' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, partition_id, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {s3_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n1\t2\t3\ty", f"Unexpected exported data:\n{result}"


def test_export_partition_multi_column_partition_key_success_all(cluster, source_engine):
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["replica1"]

    postfix = str(uuid.uuid4()).replace("-", "_")
    mt_table = f"multi_pkey_ok_all_mt_table_{postfix}"
    s3_table = f"multi_pkey_ok_all_s3_table_{postfix}"

    node.query(f"""
        CREATE TABLE {mt_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = {source_engine_clause(source_engine, mt_table)}
        PARTITION BY (a, b, c)
        ORDER BY tuple()
    """)

    node.query(f"""
        CREATE TABLE {s3_table} (a Int32, b Int32, c Int32, val String)
        ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive')
        PARTITION BY (a, b, c)
    """)

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (4, 5, 6, 'y')")

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY partition_id"
    ).strip().split("\n")

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {s3_table}")

    for pid in partition_ids:
        wait_for_export_status(node, mt_table, s3_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {s3_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {s3_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n4\t5\t6\ty", f"Unexpected exported data:\n{result}"
