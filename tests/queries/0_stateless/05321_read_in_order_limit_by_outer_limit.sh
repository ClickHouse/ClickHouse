#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# no-random-settings: the assertion counts the rows the read-in-order pool reads, which the block
# size and the read-in-order settings take part in.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Without `ORDER BY`, `LIMIT BY` drives the read in order itself (`optimizeLimitByInOrder`). An
# enclosing `LIMIT` has to reach the reader as the soft-limit threshold, as it does from a
# `SortingStep`: it makes the in-order pool hand out a single range as the first task instead of the
# whole range set. Without it both parts below are read in full (40000 rows), with it the read stops
# after the first part (~20000 rows). Checked both for a direct read and through a `Merge` table.
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_limit_by_outer_limit;
DROP TABLE IF EXISTS t_limit_by_outer_limit_merge;

CREATE TABLE t_limit_by_outer_limit (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_limit_by_outer_limit;

INSERT INTO t_limit_by_outer_limit SELECT number, number FROM numbers(20000);
INSERT INTO t_limit_by_outer_limit SELECT number, number FROM numbers(20000, 20000);

CREATE TABLE t_limit_by_outer_limit_merge AS t_limit_by_outer_limit
ENGINE = Merge(currentDatabase(), '^t_limit_by_outer_limit\$');
"

SETTINGS="--optimize_read_in_order=1 --optimize_limit_by_in_order=1 --max_threads=4"

for table in t_limit_by_outer_limit t_limit_by_outer_limit_merge
do
    QUERY_ID="05321_${table}_${CLICKHOUSE_DATABASE}"
    $CLICKHOUSE_CLIENT ${SETTINGS} --query_id "$QUERY_ID" -q "SELECT key FROM $table LIMIT 1 BY key LIMIT 10 FORMAT Null"

    # The bound only has to separate a single-range first task from a whole-range one.
    $CLICKHOUSE_CLIENT -n -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT '$table', sum(read_rows) < 30000 FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'
      AND event_date >= yesterday();
    "

    # The answer must not depend on the task sizing.
    $CLICKHOUSE_CLIENT ${SETTINGS} -q "SELECT count(), sum(key) FROM (SELECT key FROM $table LIMIT 1 BY key LIMIT 10)"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_limit_by_outer_limit_merge"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_limit_by_outer_limit"
