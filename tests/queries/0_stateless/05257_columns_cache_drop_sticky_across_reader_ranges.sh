#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide, and SYSTEM DROP COLUMNS CACHE clears the
# cache of every other test.
# no-fasttest: a test that arms a fail point runs alone, and such tests are kept out of the fast test.
# no-random-settings: the read has to stay one task, so that both mark ranges go through one reader.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP COLUMNS CACHE` must be sticky against a reader that was already running when it
# was issued, also when the reader moves on to a later mark range after the drop: the reader has
# to keep the invalidation generation it captured when it was created, and not pick up the
# post-drop one when the next range begins, otherwise it repopulates the cache right after the
# drop.

FP=columns_cache_reader_pause_before_later_range

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# Two mark ranges of one part, [0, 10) and [90, 100), read by one reader.
QUERY="SELECT count(), sum(v) FROM t_cc_sticky WHERE k < 100 OR k >= 900
    SETTINGS use_columns_cache = 1, max_threads = 1, optimize_move_to_prewhere = 0, use_query_condition_cache = 0"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_cc_sticky;
    CREATE TABLE t_cc_sticky (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 10, index_granularity_bytes = 0;
    INSERT INTO t_cc_sticky SELECT number, number * 2 FROM numbers(1000);
"

# Control: without a drop in the middle, the read populates the cache.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "$QUERY"
$CLICKHOUSE_CLIENT --query "SELECT 'cached without a drop', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_sticky'"

# The reader stops right before its second range, the cache is dropped, and the reader goes on.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query "$QUERY" &
query_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $query_pid

$CLICKHOUSE_CLIENT --query "SELECT 'cached after a drop in the middle', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_sticky'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_cc_sticky"
