import pytest

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str


# Exactly-once incremental refreshable MV writing MergeTree -> Iceberg, over local filesystem and S3
# (MinIO). The target is a no-catalog Iceberg table, whose commit is an atomic `if-none-match` swap of
# the metadata pointer, so the advanced cursor -- embedded in the append snapshot's summary -- commits
# atomically with the data files. The database is Atomic, so there is NO Keeper coordination znode: the
# only durable place the cursor can live is the Iceberg snapshot (in MinIO for storage_type=s3). A kill
# between rounds therefore proves the cursor is read back from the table, i.e. exactly-once.
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_incremental_refreshable_mv_iceberg_exactly_once(started_cluster_iceberg_no_spark, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    suffix = storage_type + "_" + get_uuid_str()
    src = f"irmv_src_{suffix}"
    tgt = f"irmv_tgt_{suffix}"
    mv = f"irmv_mv_{suffix}"

    # MergeTree source with the block-number/offset columns the streaming cursor reads.
    instance.query(
        f"""
        CREATE TABLE {src} (k Int64)
        ENGINE = MergeTree ORDER BY k
        SETTINGS
            enable_block_number_column = 1,
            enable_block_offset_column = 1,
            add_minmax_index_for_block_number_column = 1,
            add_minmax_index_for_block_offset_column = 1,
            part_minmax_index_columns = 'with_block_number_offset'
        """
    )

    create_iceberg_table(storage_type, instance, tgt, started_cluster_iceberg_no_spark, "(k Int64)")

    # REFRESH EVERY 10 YEAR + EMPTY: no automatic refresh; every refresh below is triggered manually.
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {mv}
            REFRESH EVERY 10 YEAR SETTINGS refresh_incremental = 1 APPEND
            TO {tgt} EMPTY
            AS SELECT k FROM {src}
        """
    )

    # Round 1: commit rows 0..4 and refresh. The advanced cursor is embedded in the Iceberg append snapshot.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(5)")
    instance.query(f"SYSTEM REFRESH VIEW {mv}")
    instance.query(f"SYSTEM WAIT VIEW {mv}")
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "5\t5"

    # The cursor must be committed inside the snapshot summary (not Keeper). Read it back from the table.
    cursor = instance.query(
        f"SELECT summary['clickhouse.refresh_cursor'] FROM system.iceberg_history "
        f"WHERE database = 'default' AND table = '{tgt}' ORDER BY made_current_at DESC LIMIT 1"
    ).strip()
    assert cursor != "", "refresh cursor was not embedded in the Iceberg snapshot summary"

    # Restart wipes all in-memory RefreshTask state. Only the cursor persisted in the Iceberg snapshot
    # summary can let the next refresh resume instead of re-reading from the beginning.
    instance.restart_clickhouse()

    # Round 2: commit rows 5..9 and refresh. If the cursor survived (Iceberg), only the 5 new rows are
    # appended -> 10 rows, 10 distinct (exactly-once). If it were lost, round 2 re-reads all 10 -> 15 rows.
    instance.query(f"INSERT INTO {src} SELECT number FROM numbers(5, 5)")
    instance.query(f"SYSTEM REFRESH VIEW {mv}")
    instance.query(f"SYSTEM WAIT VIEW {mv}")
    assert instance.query(f"SELECT count(), uniqExact(k) FROM {tgt}").strip() == "10\t10"

    instance.query(f"DROP TABLE {mv}")
    instance.query(f"DROP TABLE {src}")
    instance.query(f"DROP TABLE {tgt}")
