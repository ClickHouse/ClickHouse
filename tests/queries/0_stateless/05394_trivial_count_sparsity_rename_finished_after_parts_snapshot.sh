#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: uses a server-wide failpoint that pauses every lookup of column defaultness stats.
# no-fasttest: tests that enable a fail point run alone, and they are kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `optimize_trivial_count_with_sparsity_filter` answers `count() WHERE x = 0` from `num_defaults` in
# `serialization.json`, keyed by the on-disk column names. Here a pending `DROP COLUMN x, RENAME COLUMN y TO x`
# finishes after the unmutated part was captured for the defaultness stats, but before the stats are
# consumed. The live counter of pending metadata mutations is zero by then, while the captured part still
# stores the dropped `x` (all zeros) under the queried name, so the rewrite must still be rejected.

FAILPOINT="mt_column_defaultness_stats_pause_after_parts_snapshot"
TABLE="t_sparsity_rename_finished_after_parts_snapshot"
OUTPUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_${TABLE}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    wait || true
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
    rm -f "$OUTPUT"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;

    CREATE TABLE $TABLE (x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, compute_exact_num_defaults_for_sparse_columns = 1;

    SYSTEM STOP MERGES $TABLE;
    INSERT INTO $TABLE VALUES (0, 100), (0, 200);

    ALTER TABLE $TABLE DROP COLUMN x, RENAME COLUMN y TO x SETTINGS alter_sync = 0, mutations_sync = 0;

    SYSTEM ENABLE FAILPOINT $FAILPOINT;
"

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM $TABLE WHERE x = 0
    SETTINGS optimize_trivial_count_query = 1, optimize_trivial_count_with_sparsity_filter = 1,
        enable_analyzer = 1, enable_parallel_replicas = 0, make_distributed_plan = 0, empty_result_for_aggregation_by_empty_set = 0
" > "$OUTPUT" &

# The settings above make the query reach the lookup of the defaultness stats, the timeout only turns
# a regression of that into a failure instead of a hang.
timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"

# Complete the mutation while the query holds the unmutated part.
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
for _ in $(seq 1 600)
do
    pending=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done")
    [[ "$pending" == "0" ]] && break
    sleep 0.1
done
echo "pending mutations: $pending"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait

echo "count with the rewrite: $(cat "$OUTPUT")"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $TABLE WHERE x = 0 SETTINGS optimize_trivial_count_with_sparsity_filter = 0"
