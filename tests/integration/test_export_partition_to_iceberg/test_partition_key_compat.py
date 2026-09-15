import pytest

from helpers.export_partition_helpers import (
    REJECTED_PARTITION_EXPORT_CASES as SHARED_REJECTED_PARTITION_EXPORT_CASES,
)
from helpers.export_partition_helpers import (
    RejectedPartitionExportCase,
    first_partition_id,
    make_iceberg_s3,
    make_source,
    unique_suffix,
    wait_for_export_status,
)
from helpers.iceberg_export_stats import fetch_manifest_entries

from .common import (
    data_file_partition_records,
    partition_scalar,
)

CLUSTER_INSTANCES = ["replica1"]

# The Iceberg partition-compatibility gate: which source partition keys may be exported into which
# destination transform, and whether the partition metadata the commit writes matches the data.
# Rejections are synchronous.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_source_mt(node, mt_table: str, replica_name: str, engine: str = "ReplicatedMergeTree"):
    make_source(node, mt_table, "id Int64, year Int32", "year",
                engine=engine, replica_name=replica_name)


def test_partition_transform_compatibility_accepted(cluster, source_engine):
    """
    Verify that EXPORT PARTITION is accepted (no BAD_ARGUMENTS) for every
    supported transform when the MergeTree and Iceberg partition specs match.

    Cases covered:
    1. Compound identity (year, region), exported to a spec that lists the fields in reverse order
    2. Year transform  – toYearNumSinceEpoch(event_date)
    3. Month transform – toMonthNumSinceEpoch(event_date)
    4. truncate[4]     – icebergTruncate(4, category)
    5. bucket[8]       – icebergBucket(8, user_id)
    6. Compound mixed  – (toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()

    def check_accepted(mt, iceberg, description):
        pid = first_partition_id(node, mt)
        node.query(
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}",
            settings={"allow_insert_into_iceberg": 1},
        )
        return pid

    # 1. Compound identity, with the destination listing the fields in the opposite order: the
    # source key pins both columns, so the partition is single-valued for either field order.
    cols = "id Int64, year Int32, region String"
    t = f"mt_acc_1_{uid}"; i = f"iceberg_acc_1_{uid}"
    make_source(node, t, cols, "(year, region)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 2023, 'EU')")
    make_iceberg_s3(node, i, cols, "(region, year)")
    pid = check_accepted(t, i, "compound identity (year, region)")
    wait_for_export_status(node, t, i, pid, "COMPLETED")
    count = int(node.query(f"SELECT count() FROM {i}").strip())
    assert count == 1, f"[compound identity (year, region)] Expected 1 row in Iceberg table, got {count}"
    result = node.query(f"SELECT id, year, region FROM {i}").strip()
    assert result == "1\t2023\tEU", f"[compound identity (year, region)] Unexpected exported data:\n{result}"

    # 2. Year transform
    cols = "id Int64, event_date Date"
    t = f"mt_acc_2_{uid}"; i = f"iceberg_acc_2_{uid}"
    make_source(node, t, cols, "toYearNumSinceEpoch(event_date)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, '2020-06-15')")
    make_iceberg_s3(node, i, cols, "toYearNumSinceEpoch(event_date)")
    check_accepted(t, i, "year transform")

    # 3. Month transform
    cols = "id Int64, event_date Date"
    t = f"mt_acc_3_{uid}"; i = f"iceberg_acc_3_{uid}"
    make_source(node, t, cols, "toMonthNumSinceEpoch(event_date)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, '2020-06-15')")
    make_iceberg_s3(node, i, cols, "toMonthNumSinceEpoch(event_date)")
    check_accepted(t, i, "month transform")

    # 4. truncate[4]
    cols = "id Int64, category String"
    t = f"mt_acc_4_{uid}"; i = f"iceberg_acc_4_{uid}"
    make_source(node, t, cols, "icebergTruncate(4, category)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 'clickhouse')")
    make_iceberg_s3(node, i, cols, "icebergTruncate(4, category)")
    check_accepted(t, i, "truncate[4]")

    # 5. bucket[8]
    cols = "id Int64, user_id Int64"
    t = f"mt_acc_5_{uid}"; i = f"iceberg_acc_5_{uid}"
    make_source(node, t, cols, "icebergBucket(8, user_id)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 42)")
    make_iceberg_s3(node, i, cols, "icebergBucket(8, user_id)")
    check_accepted(t, i, "bucket[8]")

    # 6. Compound mixed: year(event_date) + bucket[16](user_id)
    cols = "id Int64, event_date Date, user_id Int64"
    t = f"mt_acc_6_{uid}"; i = f"iceberg_acc_6_{uid}"
    make_source(node, t, cols, "(toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, '2021-03-01', 99)")
    make_iceberg_s3(node, i, cols, "(toYearNumSinceEpoch(event_date), icebergBucket(16, user_id))")
    check_accepted(t, i, "compound year+bucket[16]")


def test_partition_transform_compatibility_rejected(cluster, source_engine):
    """
    Verify that partition specs that cannot be exported are rejected with BAD_ARGUMENTS.

    Acceptance is data-dependent: a source partition must map to a single Iceberg partition. The
    mismatch cases below therefore use data that makes the source partition span several
    destination partitions (a single-row partition would be trivially single-valued and accepted).

    Cases covered:
    1. Transform mismatch on the same column: year-transform source vs identity destination, where
       the year partition contains several distinct dates.
    2. Bucket count mismatch: bucket[8] vs bucket[16] (bucket is non-monotonic, always structural).
    3. Truncate width mismatch: truncate[4] source vs truncate[8] destination, with values sharing
       the 4-char prefix but differing within the first 8 chars.
    4. Unsupported MergeTree expression (intDiv) vs identity, with one bucket spanning several years.
    5. Destination partitions by a column that is not in the source partition key.
    """
    node = cluster.instances["replica1"]
    uid = unique_suffix()

    def assert_rejected(mt, iceberg, description):
        pid = first_partition_id(node, mt)
        error = node.query_and_get_error(
            f"ALTER TABLE {mt} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg}",
            settings={"allow_insert_into_iceberg": 1},
        )
        assert "BAD_ARGUMENTS" in error, (
            f"[{description}] Expected BAD_ARGUMENTS, got: {error!r}"
        )

    # 1. Transform mismatch: MergeTree year-transform, Iceberg identity on same Date col
    cols = "id Int64, event_date Date"
    t = f"mt_rej_1_{uid}"; i = f"iceberg_rej_1_{uid}"
    make_source(node, t, cols, "toYearNumSinceEpoch(event_date)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, '2020-01-01'), (2, '2020-12-31')")
    make_iceberg_s3(node, i, cols, "event_date")   # identity, not year-transform
    assert_rejected(t, i, "year-transform source vs identity destination")

    # 2. Bucket count mismatch: bucket[8] vs bucket[16]
    cols = "id Int64, user_id Int64"
    t = f"mt_rej_2_{uid}"; i = f"iceberg_rej_2_{uid}"
    make_source(node, t, cols, "icebergBucket(8, user_id)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 42)")
    make_iceberg_s3(node, i, cols, "icebergBucket(16, user_id)")
    assert_rejected(t, i, "bucket[8] vs bucket[16]")

    # 3. Truncate width mismatch: values share the 4-char prefix but differ within 8 chars.
    cols = "id Int64, category String"
    t = f"mt_rej_3_{uid}"; i = f"iceberg_rej_3_{uid}"
    make_source(node, t, cols, "icebergTruncate(4, category)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 'clickhouse'), (2, 'clickfmt')")
    make_iceberg_s3(node, i, cols, "icebergTruncate(8, category)")
    assert_rejected(t, i, "truncate[4] source vs truncate[8] destination")

    # 4. Unsupported MergeTree expression vs identity: one intDiv bucket spans several years.
    cols = "id Int64, year Int32"
    t = f"mt_rej_4_{uid}"; i = f"iceberg_rej_4_{uid}"
    make_source(node, t, cols, "intDiv(year, 100)", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 2000), (2, 2099)")
    make_iceberg_s3(node, i, cols, "year")
    assert_rejected(t, i, "intDiv source vs identity destination")

    # 5. Destination partitions by a column absent from the source partition key.
    cols = "id Int64, year Int32"
    t = f"mt_rej_5_{uid}"; i = f"iceberg_rej_5_{uid}"
    make_source(node, t, cols, "year", engine=source_engine)
    node.query(f"INSERT INTO {t} VALUES (1, 2020)")
    make_iceberg_s3(node, i, cols, "id")   # identity on id, which the source does not partition by
    assert_rejected(t, i, "destination partitions by a non-source-key column")


def test_partition_key_compatibility_check(cluster, source_engine):
    """
    Verify that EXPORT PARTITION throws BAD_ARGUMENTS synchronously when the
    MergeTree partition key does not match the Iceberg table's partition spec,
    and is accepted without error when the destination is satisfiable.

    Three cases:
    1. Column mismatch   – MergeTree PARTITION BY year, Iceberg PARTITION BY id (must be rejected)
    2. Unpartitioned dst – MergeTree PARTITION BY year, Iceberg unpartitioned (accepted: the source is
                           flattened into the single empty Iceberg partition)
    3. Matching keys     – both PARTITION BY year (must be accepted)
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"

    create_source_mt(node, mt_table, "replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2021)")

    # --- Case 1: Iceberg partitioned by 'id' but MergeTree by 'year' ---
    iceberg_col_mismatch = f"iceberg_col_mismatch_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_col_mismatch}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_col_mismatch}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        PARTITION BY id SETTINGS s3_retry_attempts = 3
        """
    )
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_col_mismatch}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for partition column mismatch, got: {error!r}"
    )

    # --- Case 2: Iceberg unpartitioned, MergeTree PARTITION BY year ---
    # An unpartitioned Iceberg table has a single (empty) partition, so a partitioned source is
    # flattened into it and the export is accepted; the partition-column values survive as data.
    iceberg_unpartitioned = f"iceberg_unpartitioned_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_unpartitioned}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_unpartitioned}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        SETTINGS s3_retry_attempts = 3
        """
    )
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_unpartitioned}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_unpartitioned, "2020", "COMPLETED")
    count = int(node.query(f"SELECT count() FROM {iceberg_unpartitioned}").strip())
    assert count == 2, f"Expected 2 rows in unpartitioned Iceberg table after export, got {count}"
    result = node.query(f"SELECT id, year FROM {iceberg_unpartitioned} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020", f"Unexpected data in unpartitioned Iceberg table:\n{result}"

    # --- Case 3: Matching partition keys (both PARTITION BY year) ---
    iceberg_match = f"iceberg_match_{uid}"
    node.query(
        f"""
        CREATE TABLE {iceberg_match}
        (id Int64, year Int32)
        ENGINE = IcebergS3(
            'http://minio1:9001/root/data/{iceberg_match}/',
            'minio',
            'ClickHouse_Minio_P@ssw0rd'
        )
        PARTITION BY year SETTINGS s3_retry_attempts = 3
        """
    )
    # Should not raise — the check passes so the export is accepted synchronously
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_match}",
        settings={"allow_insert_into_iceberg": 1},
    )


def test_partition_transform_equivalence_gate(cluster, source_engine):
    """
    The Iceberg partition-compatibility gate accepts a source partition key whose transform is
    equivalent to (or finer than) the destination Iceberg transform when the exported partition is
    provably single-valued for every destination field, and rejects it otherwise. Accept cases are
    verified end-to-end (data + metadata); reject cases must throw BAD_ARGUMENTS synchronously.
    """
    node = cluster.instances["replica1"]
    dt = "id Int64, event_time DateTime"
    yr = "id Int64, year Int32, region String"

    cases = [
        # toDate -> day: rows within one day map to a single Iceberg day partition.
        {"name": "todate_day", "columns": dt, "source_key": "toDate(event_time)",
         "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')", "expect_ok": True},
        # toYYYYMM -> month: different days of the same month map to a single month partition.
        {"name": "toyyyymm_month", "columns": dt, "source_key": "toYYYYMM(event_time)",
         "dest_key": "toMonthNumSinceEpoch(event_time)",
         "rows": "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')", "expect_ok": True},
        # toStartOfHour -> hour.
        {"name": "startofhour_hour", "columns": dt, "source_key": "toStartOfHour(event_time)",
         "dest_key": "toRelativeHourNum(event_time)",
         "rows": "(1, '2024-03-05 12:00:00'), (2, '2024-03-05 12:59:00')", "expect_ok": True},
        # Finer source (day + country) into a day-partitioned destination: extra column allowed.
        {"name": "finer_day", "columns": "id Int64, event_time DateTime, country String",
         "source_key": "(toDate(event_time), country)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00', 'US'), (2, '2024-03-05 20:00:00', 'US')",
         "expect_ok": True},
        # Compound field order reversed: matching is by column; the destination defines tuple order.
        {"name": "reversed_order", "columns": yr, "source_key": "(year, region)",
         "dest_key": "(region, year)", "rows": "(1, 2020, 'EU')", "expect_ok": True,
         "verify": [("region", "region"), ("year", "year")]},
        # Superset source: (year, region) into a year-only destination is finer, so accepted.
        {"name": "superset", "columns": yr, "source_key": "(year, region)", "dest_key": "year",
         "rows": "(1, 2020, 'EU')", "expect_ok": True, "verify": [("year", "year")]},
        # Coarser source: a month partition spans several days, so it cannot map to one day.
        {"name": "coarser_day", "columns": dt, "source_key": "toYYYYMM(event_time)",
         "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')", "expect_ok": False},
        # A hash is never monotonic, so min/max cannot prove anything about it, but an identity source key
        # pins k within the partition and a bucket of a single value is a single bucket.
        {"name": "bucket_from_identity_source", "columns": "id Int64, k Int64", "source_key": "k",
         "dest_key": "icebergBucket(8, k)", "rows": "(1, 10), (2, 10)", "expect_ok": True,
         "verify": [("k", "icebergBucket(8, k)")]},
        # The same bucket destination over a source key that does not pin k: nothing proves the rows of one
        # source partition hash into the same bucket.
        {"name": "bucket_needs_structural", "columns": "id Int64, k Int64",
         "source_key": "intDiv(k, 100)", "dest_key": "icebergBucket(8, k)",
         "rows": "(1, 10), (2, 20)", "expect_ok": False},
        # Identical expressions on a Nullable column: accepted structurally. The min/max proof refuses
        # Nullable (a NULL forms its own destination partition and the endpoints cannot rule it out),
        # so this only passes because the source already groups by exactly this transform. DateTime64(6)
        # round-trips through the Iceberg schema unchanged, which the structural type check requires.
        {"name": "nullable_exact_day", "columns": "id Int64, event_time Nullable(DateTime64(6))",
         "source_key": "toRelativeDayNum(event_time)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')",
         "source_settings": "allow_nullable_key = 1", "expect_ok": True},
        # Same, for identity, which is exempt from the structural type check.
        {"name": "nullable_exact_identity", "columns": "id Int64, k Nullable(Int64)",
         "source_key": "k", "dest_key": "k", "rows": "(1, 10), (2, 10)",
         "source_settings": "allow_nullable_key = 1", "expect_ok": True,
         "verify": [("k", "k")]},
        # A Nullable column without identical expressions falls to the min/max proof, which cannot see
        # NULLs, so it is rejected.
        {"name": "nullable_no_match", "columns": "id Int64, event_time Nullable(DateTime64(6))",
         "source_key": "toYYYYMM(event_time)", "dest_key": "toRelativeDayNum(event_time)",
         "rows": "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')",
         "source_settings": "allow_nullable_key = 1", "expect_ok": False},
    ]
    run_partition_compat_cases(node, cases, engine=source_engine)


def test_partition_transform_granularity_matrix(cluster, source_engine):
    """
    Exercise the common ClickHouse temporal partition keys and the granularity relationships between
    the source key and the destination Iceberg transform. Acceptance is data-dependent (a source
    partition must be single-valued for every destination field), so a coarser source can still be
    accepted when a particular partition does not actually repartition. Accept cases are verified
    end-to-end (data + metadata); reject cases must throw BAD_ARGUMENTS.
    """
    node = cluster.instances["replica1"]
    dt = "id Int64, event_time DateTime"
    same_day = "(1, '2024-03-05 01:00:00'), (2, '2024-03-05 20:00:00')"
    same_month = "(1, '2024-03-01 00:00:00'), (2, '2024-03-20 00:00:00')"
    same_year = "(1, '2024-03-05 00:00:00'), (2, '2024-09-10 00:00:00')"

    def case(name, source_key, dest_key, rows, expect_ok):
        return {"name": name, "columns": dt, "source_key": source_key, "dest_key": dest_key,
                "rows": rows, "expect_ok": expect_ok}

    cases = [
        # Common temporal keys at the same granularity as the destination transform.
        case("startofmonth_month", "toStartOfMonth(event_time)", "toMonthNumSinceEpoch(event_time)", same_month, True),
        case("yyyymmdd_day", "toYYYYMMDD(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        case("startofday_day", "toStartOfDay(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        case("toyear_year", "toYear(event_time)", "toYearNumSinceEpoch(event_time)", same_year, True),
        case("startofyear_year", "toStartOfYear(event_time)", "toYearNumSinceEpoch(event_time)", same_year, True),
        # Finer source into a coarser destination: a finer partition sits inside one coarser bucket.
        case("day_into_month", "toDate(event_time)", "toMonthNumSinceEpoch(event_time)", same_day, True),
        case("day_into_year", "toDate(event_time)", "toYearNumSinceEpoch(event_time)", same_day, True),
        case("hour_into_day", "toStartOfHour(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2024-03-05 12:00:00'), (2, '2024-03-05 12:30:00')", True),
        case("month_into_year", "toYYYYMM(event_time)", "toYearNumSinceEpoch(event_time)", same_month, True),
        # Coarser source into a finer destination: the partition spans several destination buckets.
        case("year_into_month", "toYear(event_time)", "toMonthNumSinceEpoch(event_time)",
             "(1, '2020-01-15 00:00:00'), (2, '2020-06-15 00:00:00')", False),
        case("year_into_day", "toYear(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2020-01-01 00:00:00'), (2, '2020-12-31 00:00:00')", False),
        # Same coarse/fine pair, but this year partition holds a single day, so it does not
        # repartition and is accepted - acceptance depends on the data, not the structure.
        case("year_into_day_single_day", "toYear(event_time)", "toRelativeDayNum(event_time)", same_day, True),
        # Weekly has no Iceberg equivalent: a week partition holding two days cannot map to one day.
        case("week_into_day", "toMonday(event_time)", "toRelativeDayNum(event_time)",
             "(1, '2024-03-05 00:00:00'), (2, '2024-03-07 00:00:00')", False),
    ]
    run_partition_compat_cases(node, cases, engine=source_engine)


def test_partition_multicolumn_subset(cluster, source_engine):
    """
    Destination partition columns must be a subset of the source partition-key columns. A wide
    source whose partition key is a superset of the destination's is accepted (and its multi-column
    data plus per-field metadata verified); a destination partitioning by a column absent from the
    source partition key is rejected.
    """
    node = cluster.instances["replica1"]
    wide = "id Int64, event_time DateTime, region String, tenant Int32, v1 Float64, v2 String"

    cases = [
        # Destination partition columns {event_time, region} are a strict subset of the source's
        # {event_time, region, tenant}: accepted, with multi-column data and per-field metadata.
        {"name": "subset_ok", "columns": wide,
         "source_key": "(toDate(event_time), region, tenant)",
         "dest_key": "(toRelativeDayNum(event_time), region)",
         "rows": "(1, '2024-03-05 01:00:00', 'US', 7, 1.5, 'a'), "
                 "(2, '2024-03-05 20:00:00', 'US', 7, 2.5, 'b')",
         "expect_ok": True,
         "verify": [("event_time", "toRelativeDayNum(event_time)"), ("region", "region")]},
        # Destination partitions by 'region', which is not in the source partition key: rejected.
        {"name": "not_subset", "columns": "id Int64, event_time DateTime, region String",
         "source_key": "toDate(event_time)",
         "dest_key": "(toRelativeDayNum(event_time), region)",
         "rows": "(1, '2024-03-05 01:00:00', 'US'), (2, '2024-03-05 20:00:00', 'EU')",
         "expect_ok": False},
    ]
    run_partition_compat_cases(node, cases, engine=source_engine)


def test_export_partition_todate_source_matches_day_metadata(cluster, source_engine):
    """
    End-to-end: a source partitioned by toDate(event_time) exports into a day-partitioned Iceberg
    table through the min/max refinement, and the day value written to the Iceberg metadata matches
    the exported data.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_todate_{uid}"
    iceberg_table = f"iceberg_todate_{uid}"

    make_source(node, mt_table, "id Int64, event_time DateTime", "toDate(event_time)",
             replica_name="replica1", engine=source_engine)
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05 01:00:00'), (2, '2024-03-05 12:00:00'), (3, '2024-03-05 23:00:00')"
    )
    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    expected_day = int(node.query(
        f"SELECT DISTINCT toRelativeDayNum(event_time) FROM {iceberg_table}"
    ).strip())

    query_id = f"todate_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_days = {int(partition_scalar(p, "event_time")) for p in partitions}
    assert meta_days == {expected_day}, (
        f"Metadata day {meta_days} must equal toRelativeDayNum {expected_day}."
    )


def test_export_partition_day_source_into_year_metadata(cluster, source_engine):
    """
    End-to-end: a source partitioned by toDate(event_time) (finer) exports into a year-partitioned
    Iceberg destination (coarser). The value written to the Iceberg metadata is the year computed by
    the destination transform over the data, not the source day.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_day_year_{uid}"
    iceberg_table = f"iceberg_day_year_{uid}"

    make_source(node, mt_table, "id Int64, event_time DateTime", "toDate(event_time)",
             replica_name="replica1", engine=source_engine)
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05 01:00:00'), (2, '2024-03-05 12:00:00'), (3, '2024-03-05 23:00:00')"
    )
    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime",
                    partition_by="toYearNumSinceEpoch(event_time)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    expected_year = int(node.query(
        f"SELECT DISTINCT toYearNumSinceEpoch(event_time) FROM {iceberg_table}"
    ).strip())

    query_id = f"day_year_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_years = {int(partition_scalar(p, "event_time")) for p in partitions}
    assert meta_years == {expected_year}, (
        f"Metadata year {meta_years} must equal toYearNumSinceEpoch {expected_year}."
    )


def test_export_partition_lossy_cast_dynamic_accept(cluster, source_engine):
    """
    A lossy Int64 -> Int32 partition-column cast is accepted by the dynamic proof when the
    partition's values fit the destination type and map to a single Iceberg bucket. Source and
    destination use different truncate widths, so the field is proven via min/max rather than a
    structural match.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_lossy_{uid}"
    iceberg_table = f"iceberg_lossy_{uid}"

    make_source(node, mt_table, "id Int64, val Int64", "icebergTruncate(10, val)",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 100), (2, 109)")
    make_iceberg_s3(node, iceberg_table, "id Int64, val Int32",
                    partition_by="icebergTruncate(1000000, val)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")
    assert int(node.query(f"SELECT count() FROM {iceberg_table}").strip()) == 2


# The shared cases plus one that only an Iceberg destination can express: a transform whose
# partition-key columns are listed in a different order on either side.
REJECTED_PARTITION_EXPORT_CASES = SHARED_REJECTED_PARTITION_EXPORT_CASES + [
    pytest.param(
        RejectedPartitionExportCase(
            src_columns="other_id Int64, user_id Int64",
            src_partition_by="icebergBucket(8, user_id)",
            dst_columns="user_id Int64, other_id Int64",
            dst_partition_by="icebergBucket(8, user_id)",
            insert_values="(1, 42)",
            error_substrings=("partition key column",),
        ),
        id="transform_partition_key_different_column_order",
    ),
]


@pytest.mark.parametrize("case", REJECTED_PARTITION_EXPORT_CASES)
def test_export_partition_partition_key_mismatch_variants_are_rejected(cluster, case, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_rejected_{uid}"
    iceberg_table = f"iceberg_rejected_{uid}"

    make_source(node, mt_table, case.src_columns, case.src_partition_by, replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node, iceberg_table, case.dst_columns, partition_by=case.dst_partition_by)

    node.query(f"INSERT INTO {mt_table} VALUES {case.insert_values}")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS, got: {error}"
    for substring in case.error_substrings:
        assert substring in error, f"Expected {substring!r} in error, got: {error}"

    error_all = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error_all, f"Expected BAD_ARGUMENTS, got: {error_all}"

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected 0 rows in destination after rejected export, got {count}"


def test_export_partition_multi_column_partition_key_success_all(cluster, source_engine):
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_multi_pkey_ok_all_{uid}"
    iceberg_table = f"iceberg_multi_pkey_ok_all_{uid}"

    cols = "a Int32, b Int32, c Int32, val String"
    make_source(node, mt_table, cols, "(a, b, c)", replica_name="replica1", engine=source_engine)
    make_iceberg_s3(node, iceberg_table, cols, partition_by="(a, b, c)")

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2, 3, 'x'), (4, 5, 6, 'y')")

    partition_ids = node.query(
        f"SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() "
        f"AND table = '{mt_table}' AND active ORDER BY partition_id"
    ).strip().split("\n")

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )

    for pid in partition_ids:
        wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows in destination after export, got {count}"

    result = node.query(f"SELECT a, b, c, val FROM {iceberg_table} ORDER BY val").strip()
    assert result == "1\t2\t3\tx\n4\t5\t6\ty", f"Unexpected exported data:\n{result}"


def assert_iceberg_partition_metadata(node, iceberg_table, uid, fields):
    """Assert every data-file partition record's field equals the single DISTINCT value of the
    corresponding expression over the exported destination data. `fields` is a list of
    (metadata_field_name, value_expr). String-normalized so integer transforms and identity
    string/int fields compare uniformly."""
    query_id = f"verify_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    for field_name, value_expr in fields:
        expected = node.query(
            f"SELECT DISTINCT toString({value_expr}) FROM {iceberg_table}"
        ).strip()
        got = {str(partition_scalar(p, field_name)) for p in partitions}
        assert got == {expected}, (
            f"metadata field {field_name!r} = {got}, expected {{{expected!r}}}"
        )


def run_partition_compat_cases(node, cases, engine: str = "ReplicatedMergeTree"):
    """Run partition-compatibility cases against the Iceberg export gate.

    Reject cases (``expect_ok=False``) are checked synchronously - the gate fires while scheduling,
    so the ALTER throws immediately. Accept cases are dispatched together, then awaited, then their
    data (full ordered row comparison against the exported source partition) and Iceberg partition
    metadata are verified. Each case is a dict: name, columns, source_key, dest_key, rows, expect_ok,
    and optional verify (list of (metadata_field_name, value_expr); defaults to
    [("event_time", dest_key)]) and source_settings (extra MergeTree settings)."""
    settings = {"allow_insert_into_iceberg": 1}

    def setup(case):
        uid = unique_suffix()
        mt_table = f"mt_{case['name']}_{uid}"
        iceberg_table = f"iceberg_{case['name']}_{uid}"
        make_source(node, mt_table, case["columns"], case["source_key"], replica_name="replica1",
                 extra_settings=case.get("source_settings", ""), engine=engine)
        node.query(f"INSERT INTO {mt_table} VALUES {case['rows']}")
        make_iceberg_s3(node, iceberg_table, case["columns"], partition_by=case["dest_key"])
        pid = first_partition_id(node, mt_table)
        return uid, mt_table, iceberg_table, pid

    for case in cases:
        if case["expect_ok"]:
            continue
        _uid, mt_table, iceberg_table, pid = setup(case)
        error = node.query_and_get_error(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
            settings=settings,
        )
        assert "BAD_ARGUMENTS" in error, f"{case['name']}: expected BAD_ARGUMENTS, got: {error!r}"

    dispatched = []
    for case in cases:
        if not case["expect_ok"]:
            continue
        uid, mt_table, iceberg_table, pid = setup(case)
        node.query(
            f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
            settings=settings,
        )
        dispatched.append((case, uid, mt_table, iceberg_table, pid))

    for case, uid, mt_table, iceberg_table, pid in dispatched:
        wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    for case, uid, mt_table, iceberg_table, pid in dispatched:
        # Export is a positional cast into the destination schema, so verify the destination equals
        # the source cast into the destination column types. Normalizing to the destination types
        # tolerates legitimate Iceberg type promotion (e.g. DateTime is stored as a microsecond
        # timestamp and returns as DateTime64(6)) while preserving destination precision, so a
        # spurious sub-second value would still surface as a mismatch.
        col_defs = node.query(
            f"SELECT name, type FROM system.columns "
            f"WHERE database = currentDatabase() AND table = '{iceberg_table}' ORDER BY position"
        ).strip().split("\n")
        projection = ", ".join(
            f"CAST({name} AS {ctype})" for name, ctype in (c.split("\t") for c in col_defs)
        )
        src = node.query(f"SELECT {projection} FROM {mt_table} ORDER BY id")
        dst = node.query(f"SELECT {projection} FROM {iceberg_table} ORDER BY id")
        assert src == dst, f"{case['name']}: destination rows differ from source"
        fields = case.get("verify") or [("event_time", case["dest_key"])]
        assert_iceberg_partition_metadata(node, iceberg_table, f"{case['name']}_{uid}", fields)


def test_export_partition_bucket_type_change_rejected(cluster, source_engine):
    """A bucket[N] partition column whose type changes (Int64 -> String) is rejected. The source
    hashLong grouping differs from the destination murmur(String) grouping, so a single source bucket
    can fan out across several destination buckets; bucket is not order-preserving, so this cannot be
    proven dynamically and must be rejected. This previously slipped through the structural fast path,
    which matched on transform name and width while ignoring the pre-transform cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_bucket_xform_{uid}"
    iceberg_table = f"iceberg_bucket_xform_{uid}"

    make_source(node, mt_table, "id Int64, key Int64", "icebergBucket(16, key)",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 42), (2, 42)")

    make_iceberg_s3(node, iceberg_table, "id Int64, key String",
                    partition_by="icebergBucket(16, key)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a type-changing bucket transform, got: {error!r}"
    )


def test_export_partition_truncate_type_change_rejected(cluster, source_engine):
    """icebergTruncate with the same width but a changed column type (Int64 -> String) is rejected.
    Truncate is numeric on integers (120..129 -> 120) but byte-wise on strings ('120'..'129' stay
    distinct), so one source truncate bucket can map to several destination buckets. The structural
    fast path must not accept it on matching transform name and width; the dynamic proof rejects it
    because the endpoints do not collapse to a single destination value."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_trunc_xform_{uid}"
    iceberg_table = f"iceberg_trunc_xform_{uid}"

    # 120 and 129 are one Int64 truncate[10] bucket (120) but two distinct string truncations.
    make_source(node, mt_table, "id Int64, key Int64", "icebergTruncate(10, key)",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 120), (2, 129)")

    make_iceberg_s3(node, iceberg_table, "id Int64, key String",
                    partition_by="icebergTruncate(10, key)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a type-changing truncate transform, got: {error!r}"
    )


def test_export_partition_value_preserving_cast_not_order_preserving_rejected(cluster, source_engine):
    """Int64 -> String keeps every value, but not their order: 2 and 29 are the endpoints of the
    source partition, yet the interior value 10 casts to a string that sorts outside them. The
    endpoints truncate to '2' while 10 truncates to '1', so the partition spans two destination
    buckets and must be rejected instead of being waved through as a lossless cast."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_cast_order_{uid}"
    iceberg_table = f"iceberg_cast_order_{uid}"

    make_source(node, mt_table, "id Int64, k Int64", "intDiv(k, 100)",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2), (2, 10), (3, 29)")

    make_iceberg_s3(node, iceberg_table, "id Int64, k String",
                    partition_by="icebergTruncate(1, k)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a non-order-preserving cast, got: {error!r}"
    )


def test_export_partition_order_preserving_cast_accepted(cluster, source_engine):
    """The same shape as the rejected case, but with all values sharing a digit count: Int64 ->
    String is order-preserving over [20, 29], so the endpoints do bound the interior and the whole
    source partition truncates to the single destination bucket '2'."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_cast_order_ok_{uid}"
    iceberg_table = f"iceberg_cast_order_ok_{uid}"

    make_source(node, mt_table, "id Int64, k Int64", "intDiv(k, 100)",
             replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 20), (2, 25), (3, 29)")

    make_iceberg_s3(node, iceberg_table, "id Int64, k String",
                    partition_by="icebergTruncate(1, k)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    src = node.query(f"SELECT id, toString(k) FROM {mt_table} ORDER BY id").strip()
    dst = node.query(f"SELECT id, k FROM {iceberg_table} ORDER BY id").strip()
    assert src == dst, f"destination rows differ from source:\n{src}\n---\n{dst}"

    assert_iceberg_partition_metadata(node, iceberg_table, uid, [("k", "icebergTruncate(1, k)")])


def test_export_partition_timezone_mismatch_rejected(cluster, source_engine):
    """A source partitioned by day in one timezone must not be treated as structurally identical to a
    destination day computed in another timezone. The source uses Asia/Tokyo (UTC+9) and the
    destination UTC; the exported part spans a UTC-day boundary while staying within one Tokyo day, so
    it maps to two destination partitions and must be rejected."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_tzmismatch_{uid}"
    iceberg_table = f"iceberg_tzmismatch_{uid}"

    make_source(node, mt_table, "id Int64, event_time DateTime('UTC')",
             "toRelativeDayNum(event_time, 'Asia/Tokyo')", replica_name="replica1", engine=source_engine)
    # Both instants are 2024-03-05 in Tokyo (UTC+9) but 2024-03-04 and 2024-03-05 in UTC.
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-04 16:00:00'), (2, '2024-03-05 10:00:00')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime('UTC')",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1, "iceberg_partition_timezone": "UTC"},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a source/destination timezone mismatch, got: {error!r}"
    )


def test_export_partition_column_timezone_mismatch_rejected(cluster, source_engine):
    """The same mismatch as above, but with the timezone carried by the column type instead of the
    partition expression. Both sides read `toRelativeDayNum(event_time)`, so the terms are identical and
    only the types differ - and DateTime types with different timezones compare equal, so the structural
    match must not be decided by type equality alone. The part stays within one Tokyo day while spanning
    two UTC days, so it maps to two destination partitions and must be rejected.

    `iceberg_partition_timezone` is deliberately left unset: setting it stamps a timezone onto the
    destination term, which alone makes the terms differ and hides what this test covers."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_coltz_{uid}"
    iceberg_table = f"iceberg_coltz_{uid}"

    make_source(node, mt_table, "id Int64, event_time DateTime('Asia/Tokyo')",
             "toRelativeDayNum(event_time)", replica_name="replica1", engine=source_engine)
    # Both literals are 2024-03-05 in Tokyo (the column's timezone) but 2024-03-04 and 2024-03-05 in UTC.
    node.query(
        f"INSERT INTO {mt_table} VALUES (1, '2024-03-05 01:00:00'), (2, '2024-03-05 18:00:00')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_time DateTime('UTC')",
                    partition_by="toRelativeDayNum(event_time)")

    pid = first_partition_id(node, mt_table)
    error = node.query_and_get_error(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert "BAD_ARGUMENTS" in error, (
        f"Expected BAD_ARGUMENTS for a partition-column timezone mismatch, got: {error!r}"
    )


def test_export_partition_month_transform_metadata_matches_data(cluster, source_engine):
    """A month-transform partition records a months-since-epoch value in metadata that
    matches the value derived from the exported data, and a transform-filtered read
    returns the rows."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_month_xform_{uid}"
    iceberg_table = f"iceberg_month_xform_{uid}"

    make_source(node, mt_table, "id Int64, event_date Date",
             "toMonthNumSinceEpoch(event_date)", replica_name="replica1", engine=source_engine)
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05'), (2, '2024-03-20'), (3, '2024-03-31')"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_date Date",
                    partition_by="toMonthNumSinceEpoch(event_date)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    month_num = int(node.query(
        f"SELECT DISTINCT toMonthNumSinceEpoch(event_date) FROM {iceberg_table}"
    ).strip())

    query_id = f"month_xform_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_values = {int(partition_scalar(p, "event_date")) for p in partitions}
    assert meta_values == {month_num}, (
        f"Metadata month {meta_values} must equal toMonthNumSinceEpoch over the data "
        f"({month_num})."
    )

    filtered = int(node.query(
        f"SELECT count() FROM {iceberg_table} "
        f"WHERE toMonthNumSinceEpoch(event_date) = {month_num}"
    ).strip())
    assert filtered == 3, f"Transform-filtered read expected 3 rows, got {filtered}"


def test_export_partition_identity_type_change_metadata_matches_data(cluster, source_engine):
    """An identity partition column whose type changes UInt16 -> String records the
    destination String value in the Iceberg metadata, matching the exported data."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_identity_xform_{uid}"
    iceberg_table = f"iceberg_identity_xform_{uid}"

    make_source(node, mt_table, "id Int32, year UInt16", "year", replica_name="replica1", engine=source_engine)
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2024), (2, 2024)")

    make_iceberg_s3(node, iceberg_table, "id Int32, year String", partition_by="year")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 2, f"Expected 2 rows after export, got {count}"

    data_year = node.query(f"SELECT DISTINCT year FROM {iceberg_table}").strip()
    assert data_year == "2024", f"Expected exported year '2024' (String), got {data_year!r}"

    query_id = f"identity_xform_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"
    meta_values = {str(partition_scalar(p, "year")) for p in partitions}
    assert meta_values == {"2024"}, (
        f"Metadata partition {meta_values} must equal the destination String value "
        f"'2024' (not the source integer representation)."
    )


def test_export_partition_multicolumn_identity_metadata_matches_data(cluster, source_engine):
    """A multi-column identity partition (event_date Date, retention UInt64 -> Int64)
    records per-column values in the Iceberg metadata that match the exported data."""
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_multicol_{uid}"
    iceberg_table = f"iceberg_multicol_{uid}"

    # Iceberg has no unsigned types, so retention widens UInt64 -> Int64; the cast is
    # not value-preserving per canBeSafelyCast, hence the lossy opt-in below.
    make_source(node, mt_table, "id Int64, event_date Date, retention UInt64",
             "(event_date, retention)", replica_name="replica1", engine=source_engine)
    node.query(
        f"INSERT INTO {mt_table} VALUES "
        f"(1, '2024-03-05', 30), (2, '2024-03-05', 30), (3, '2024-03-05', 30)"
    )

    make_iceberg_s3(node, iceberg_table, "id Int64, event_date Date, retention Int64",
                    partition_by="(event_date, retention)")

    pid = first_partition_id(node, mt_table)
    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '{pid}' TO TABLE {iceberg_table}",
        settings={
            "allow_insert_into_iceberg": 1,
            "export_merge_tree_part_allow_lossy_cast": 1,
        },
    )
    wait_for_export_status(node, mt_table, iceberg_table, pid, "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows after export, got {count}"

    data_retention = int(node.query(
        f"SELECT DISTINCT retention FROM {iceberg_table}"
    ).strip())
    assert data_retention == 30, f"Expected exported retention 30, got {data_retention}"

    days = int(node.query(
        f"SELECT DISTINCT toInt64(event_date) FROM {iceberg_table}"
    ).strip())

    query_id = f"multicol_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table}",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )
    entries = fetch_manifest_entries(node, query_id)
    partitions = data_file_partition_records(entries)
    assert partitions, "No data-file partition records found in manifest entries"

    meta_dates = {int(partition_scalar(p, "event_date")) for p in partitions}
    assert meta_dates == {days}, (
        f"Metadata event_date {meta_dates} must equal days-since-epoch {days}."
    )
    meta_retentions = {int(partition_scalar(p, "retention")) for p in partitions}
    assert meta_retentions == {30}, (
        f"Metadata retention {meta_retentions} must equal the exported value 30."
    )

    filtered = int(node.query(
        f"SELECT count() FROM {iceberg_table} "
        f"WHERE event_date = '2024-03-05' AND retention = 30"
    ).strip())
    assert filtered == 3, f"Partition-filtered read expected 3 rows, got {filtered}"
