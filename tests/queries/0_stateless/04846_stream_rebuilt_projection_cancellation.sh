#!/usr/bin/env bash
# Tags: no-parallel
# - no-parallel: uses a global failpoint and merge blocker.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="stream_rebuilt_projection_cancellation"
FAILPOINT="merge_task_projection_stage_pause"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE" > /dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" > /dev/null 2>&1 ||:
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $TABLE
    (
        k UInt64,
        v UInt64,
        PROJECTION p_normal (SELECT k, v ORDER BY k),
        PROJECTION p_aggregate (SELECT k, sum(v) AS sum_v GROUP BY k),
        PROJECTION p_reordered (SELECT k, v ORDER BY v),
        PROJECTION p_reordered_aggregate (SELECT v % 5 AS g, sum(v) AS sum_v GROUP BY g)
    )
    ENGINE = MergeTree
    ORDER BY (k, v)
    SETTINGS
        enable_vertical_merge_algorithm = 0,
        materialize_projections_on_insert = 0,
        materialize_projections_on_merge = 1,
        merge_max_block_size = 1,
        merge_selector_algorithm = 'Manual'
"

$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $TABLE"
for offset in $(seq 0 4 44); do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO $TABLE SELECT number % 3, number + $offset FROM numbers(4)
    "
done

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL" > /dev/null 2>&1 &
optimize_pid=$!

# The reordered writer has accumulated temporary runs and `p_normal` is ready to
# stream directly. Stop the merge before any rebuilt child part is finalized.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FAILPOINT"
if wait "$optimize_pid"; then
    echo "unexpected successful cancelled merge"
    exit 1
fi

# Cancellation must leave the source parts active and publish no projection
# child part. A subsequent merge must still rebuild all projections.
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(rows)
    FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active
"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.projection_parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active
"

$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL"

$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(v), sum(k)
    FROM mergeTreeProjection(currentDatabase(), $TABLE, p_normal)
"
$CLICKHOUSE_CLIENT --query "
    SELECT k, sumMerge(\`sum(v)\`)
    FROM mergeTreeProjection(currentDatabase(), $TABLE, p_aggregate)
    GROUP BY k
    ORDER BY k
"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(v), sum(k)
    FROM mergeTreeProjection(currentDatabase(), $TABLE, p_reordered)
"
$CLICKHOUSE_CLIENT --query "
    SELECT \`modulo(v, 5)\`, sumMerge(\`sum(v)\`)
    FROM mergeTreeProjection(currentDatabase(), $TABLE, p_reordered_aggregate)
    GROUP BY \`modulo(v, 5)\`
    ORDER BY \`modulo(v, 5)\`
"
$CLICKHOUSE_CLIENT --query "
    SELECT count()
    FROM system.projection_parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active
"
$CLICKHOUSE_CLIENT --query "
    CHECK TABLE $TABLE SETTINGS check_query_single_value_result = 1
"
