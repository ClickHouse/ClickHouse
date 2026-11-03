"""Scale-shaped coverage: many snapshots and many data files.

Not a performance test — it verifies correctness of the snapshot walk,
manifest accumulation, pruning and incremental batch draining when the
metadata is dozens of snapshots deep (auto-compaction may interleave COMPACT
snapshots, which is deliberate: they must not affect any of the results).
"""

import time

from helpers.paimon_utils import (
    create_paimon_table,
    create_spark_paimon_table,
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    incremental_settings,
    query_profile_events,
    spark_table,
    upload_paimon_table,
    wait_until_query_result,
)

COMMITS = 29
ROWS_PER_COMMIT = 4  # one row per partition


def test_many_snapshots_and_files(started_cluster_paimon_with_spark):
    # s3 only: correctness at snapshot depth is storage-agnostic, and an azure
    # run would double the most expensive module in the suite.
    storage_type = "s3"
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_scale_" + storage_type + "_" + get_uuid_str()

    create_spark_paimon_table(
        spark, table_name, "id BIGINT, part INT", partitioned_by="part"
    )
    spark.sql(
        f"INSERT INTO {spark_table(table_name)} "
        f"SELECT id, id % {ROWS_PER_COMMIT} FROM range({ROWS_PER_COMMIT})"
    )
    upload_paimon_table(started_cluster, storage_type, table_name)

    # Incremental consumer established before the deep snapshot history arrives.
    create_paimon_table(
        storage_type,
        instance,
        table_name,
        started_cluster,
        additional_settings=incremental_settings(table_name),
    )
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        wait_until_query_result(instance, count_query, f"{ROWS_PER_COMMIT}\n")
        wait_until_query_result(instance, count_query, "0\n")

        # COMMITS more snapshots arrive in one upload.
        for i in range(1, COMMITS + 1):
            spark.sql(
                f"INSERT INTO {spark_table(table_name)} "
                f"SELECT id + {i * ROWS_PER_COMMIT}, id % {ROWS_PER_COMMIT} FROM range({ROWS_PER_COMMIT})"
            )
        upload_paimon_table(started_cluster, storage_type, table_name)

        total_rows = (COMMITS + 1) * ROWS_PER_COMMIT

        # Full scan through a table function: correctness over deep history.
        table_expr = get_creation_expression(
            storage_type, table_name, started_cluster, table_function=True
        )
        assert (
            instance.query(f"SELECT count(), uniqExact(id), sum(id) FROM {table_expr}")
            == f"{total_rows}\t{total_rows}\t{sum(range(total_rows))}\n"
        )
        assert (
            instance.query(f"SELECT count() FROM {table_expr} WHERE part = 2")
            == f"{total_rows // ROWS_PER_COMMIT}\n"
        )

        # Pruning still prunes with dozens of files per partition: same result
        # AND strictly fewer files read — result equality alone would also
        # hold if pruning silently stopped. Exact file counts are not
        # asserted: auto-compaction makes them nondeterministic.
        results = {}
        files_read = {}
        for pruning in (0, 1):
            settings = {"use_paimon_partition_pruning": pruning}
            results[pruning] = instance.query(
                f"SELECT sum(id) FROM {table_expr} WHERE part = 2",
                settings=settings,
            )
            # The query condition cache would skip the files the query above found to have no match.
            files_read[pruning] = query_profile_events(
                instance,
                f"SELECT sum(id) FROM {table_expr} WHERE part = 2 FORMAT Null",
                ["EngineFileLikeReadFiles"],
                settings={**settings, "use_query_condition_cache": 0},
            )[0]
        assert results[1] == results[0]
        assert files_read[1] < files_read[0], files_read

        # Incremental draining in bounded batches must deliver every pending id
        # exactly once — no replay, no drop (a count-only check cannot see a
        # replay cancelling a drop) — regardless of interleaved COMPACT snapshots.
        # Pending ids: the warmup commit wrote 0..3 (already drained by the base
        # scan); commit i wrote i*4..i*4+3, so 4..119 remain.
        pending_ids = list(range(ROWS_PER_COMMIT, (COMMITS + 1) * ROWS_PER_COMMIT))
        consumed_ids = []
        drain_query = (
            f"SELECT arraySort(groupArray(id)) FROM {table_name} "
            f"SETTINGS max_consume_snapshots = 7"
        )
        for _ in range(COMMITS + 30):
            batch = instance.query(drain_query).strip()
            batch_ids = [int(x) for x in batch.strip("[]").split(",") if x]
            consumed_ids.extend(batch_ids)
            if not batch_ids:
                if len(consumed_ids) >= len(pending_ids):
                    break
                # The background metadata refresh (1s) may not have caught up yet.
                time.sleep(0.5)
        assert sorted(consumed_ids) == pending_ids
