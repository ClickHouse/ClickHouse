#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: this test WAITS on a process-global PAUSEABLE failpoint, so a concurrent
# instance pausing or resuming the same channel would break the synchronisation.

# A `Merge` read computes one read-in-order prefix from the metadata of the tables it selected and
# then hands it to every `ReadFromMergeTree` in its child plans. An `Alias` child resolves its
# target by name twice - once when its child plan is built, once when the prefix is computed - so
# swapping the target in between makes the prefix longer than the sorting key of the read that has
# to honor it. `spreadMarkRangesAmongStreamsWithOrder` then grew the sorting-key expression list
# instead of cutting it, padding it with null `ASTPtr`s, and the server crashed while compiling it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

FP="storage_merge_create_children_plans_pause"
QID="merge_alias_prefix_${CLICKHOUSE_DATABASE}_$$"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS target;
    DROP TABLE IF EXISTS target_wider_key;
    DROP TABLE IF EXISTS a_alias;
    DROP TABLE IF EXISTS z_plain;
    DROP TABLE IF EXISTS m;

    -- The read that has to honor the prefix. Its sorting key is one column long.
    CREATE TABLE target (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a
        SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
    -- What the alias will point at once the child plan is built: two columns, so the prefix
    -- computed for the \`Merge\` is one longer than \`target\` can deliver.
    CREATE TABLE target_wider_key (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;
    -- The second child only exists to give the failpoint a second pause, i.e. a moment when the
    -- alias child plan is already built. It is named so that it is planned after the alias.
    CREATE TABLE z_plain (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b)
        SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0;

    -- Several parts each, so that the read really merges streams by the sorting-key prefix.
    INSERT INTO target SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO target SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO target SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO target_wider_key SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO z_plain SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO z_plain SELECT number % 7, number % 11 FROM numbers(60);
    INSERT INTO z_plain SELECT number % 7, number % 11 FROM numbers(60);

    CREATE TABLE a_alias ENGINE = Alias(currentDatabase(), 'target');
    CREATE TABLE m (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^(a_alias|z_plain)\$');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ${FP}"

# read_in_order_two_level_merge_threshold = 0 and read_in_order_use_virtual_row_per_block = 0 pin
# the preliminary merge that cuts the sorting key down to the announced prefix.
$CLICKHOUSE_CLIENT --query_id="${QID}" --query "
    SELECT a, b FROM m ORDER BY a, b LIMIT 5
    SETTINGS optimize_read_in_order = 1, max_threads = 4, max_block_size = 8,
             read_in_order_two_level_merge_threshold = 0, read_in_order_use_virtual_row_per_block = 0
" > "${CLICKHOUSE_TMP}/05097_result.txt" 2> "${CLICKHOUSE_TMP}/05097_error.txt" &
SELECT_PID=$!

# Paused before the first child plan. Bound every wait, so a query that never pauses fails with a
# diagnostic instead of consuming the per-test timeout.
if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the query never paused before the first child plan"
fi
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT ${FP}"

# Paused before the second child plan, so the alias child plan is built and holds a snapshot of
# the one-column `target`.
if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the query never paused before the second child plan"
fi

# Now `a_alias` resolves to a table with a two-column sorting key, while its already-built child
# plan still reads the one-column table.
$CLICKHOUSE_CLIENT --query "EXCHANGE TABLES target AND target_wider_key"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ${FP}"
timeout 120 tail --pid="${SELECT_PID}" -f /dev/null
select_rc=0
wait "${SELECT_PID}" || select_rc=$?

# A non-zero status is how the crash showed: the server died mid-query. Do not print the stderr
# contents - the harness may add `--send_logs_level`, so server log lines land there on success too.
echo "select status: ${select_rc}"
cat "${CLICKHOUSE_TMP}/05097_result.txt"
rm -f "${CLICKHOUSE_TMP}/05097_result.txt" "${CLICKHOUSE_TMP}/05097_error.txt"

# The order the children were planned in is what the test relies on: the alias child plan must be
# built before the target is swapped. `createChildrenPlans` logs one line per child.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
    SELECT 'planned: ' || multiIf(message LIKE '%a_alias%', 'a_alias', message LIKE '%z_plain%', 'z_plain', 'unexpected')
    FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND query_id = '${QID}' AND message LIKE 'Building plan for child table%'
    ORDER BY event_time_microseconds
"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE m;
    DROP TABLE a_alias;
    DROP TABLE z_plain;
    DROP TABLE target;
    DROP TABLE target_wider_key;
"
