#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-parallel: SYSTEM ENABLE FAILPOINT is process-wide, and SYSTEM DROP COLUMNS CACHE clears the
# cache of every other test.
# no-fasttest: a test that arms a fail point runs alone, and such tests are kept out of the fast test.
# no-random-settings: the reads below must keep their ranges and their single thread.

# The columns cache holds the data of a query's read after the query has ended, so that memory is the
# server's: it must neither count against the query that read it nor be credited to the query or the user
# that happens to release it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=columns_cache_reader_pause_before_later_range
user="u_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    [ -n "$holder" ] && kill "$holder" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user;
    CREATE TABLE t (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;
    INSERT INTO t SELECT number, repeat(toString(number), 5) FROM numbers(1000000);
    SYSTEM DROP COLUMNS CACHE"

# Keeps the user active, and holds next to nothing itself, while the queries below run, so that the user's
# tracker does not start a new period, which would hide a drift. Killed on the way out.
$CLICKHOUSE_CLIENT --user "$user" --function_sleep_max_microseconds_per_block 0 --query "
    SELECT count() FROM numbers(1500) WHERE sleepEachRow(0.2) = 0 SETTINGS max_block_size = 1 FORMAT Null" &
holder=$!

snapshot()
{
    $CLICKHOUSE_CLIENT --query "SELECT
        (SELECT count() FROM system.processes WHERE user = '$user'),
        (SELECT ifNull(max(memory_usage), 0) FROM system.user_processes WHERE user = '$user')"
}

base=0
for _ in {1..200}; do
    read -r running held < <(snapshot)
    if [ "${running:-0}" -ge 1 ] && [ "${held:-0}" -gt 0 ]; then
        base=$held
        break
    fi
    sleep 0.2
done
if [ "$base" -le 0 ]; then
    echo "the holding query never started"
    exit 1
fi

# About 40 MB go to the cache, twice the limit of the query that reads them: what the cache keeps is not the query's.
# The rows are narrow, so that the stripe the query holds while it reads it, about 3 MB, stays far below that limit.
$CLICKHOUSE_CLIENT --user "$user" --query "
    SELECT count() FROM t WHERE NOT ignore(v)
    SETTINGS use_columns_cache = 1, max_threads = 1, max_memory_usage = 20000000, use_query_condition_cache = 0"
$CLICKHOUSE_CLIENT --query "SELECT 'cached', sum(bytes) > 30000000 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't'"

# Whatever the read above managed to cache, the cache now holds all of it, read by a query without a limit.
$CLICKHOUSE_CLIENT --user "$user" --query "
    SELECT count() FROM t WHERE NOT ignore(v) SETTINGS use_columns_cache = 1, max_threads = 1, use_query_condition_cache = 0 FORMAT Null"

# The right side of the join is served from the cache, and the join holds those columns. The read stops before its
# second range, the cache is dropped while the join holds what it was served, and the query goes on and ends.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT --user "$user" --query "
    SELECT count() FROM numbers(10) AS a JOIN (SELECT k, v FROM t WHERE k < 300000 OR k >= 350000) AS b ON a.number = b.k
    SETTINGS use_columns_cache = 1, max_threads = 1, join_algorithm = 'hash', use_query_condition_cache = 0" &
query_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $query_pid

# Only what the holder itself works on should still be charged to the user, a few hundred kilobytes.
read -r running after < <(snapshot)
if [ "$running" -lt 1 ]; then
    echo "the holding query ended before the drift could be measured"
    exit 1
fi
drift=$(( after - base ))
if [ "${drift#-}" -lt 1500000 ]; then
    echo "the user is charged for what it holds"
else
    echo "the user drifted by $drift bytes"
fi

$CLICKHOUSE_CLIENT --query "DROP USER $user"
