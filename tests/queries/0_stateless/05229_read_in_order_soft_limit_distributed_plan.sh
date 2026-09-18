#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# no-random-settings: the assertion counts the rows the read-in-order pool reads, which the block
# size and the read-in-order settings take part in.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A bucketed distributed read rebuilds `ReadFromMergeTree` on the worker out of the read-in-order
# contract it receives, and only the prefix, the direction and the reader's own limit used to travel.
# The soft-limit threshold does not follow from that limit: a filter between the sorting and the read
# zeroes the reader's limit while the query still has an outer `LIMIT`, and it is that threshold which
# makes the in-order pool hand out a single range as the first task instead of the whole range set
# (`has_soft_limit_below_one_block`). Without it on the wire the worker reads whole parts where the
# coordinator asked for one range: ~326000 rows instead of ~10000 for the table below.
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_soft_limit;

CREATE TABLE t_soft_limit (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_soft_limit;

INSERT INTO t_soft_limit SELECT number, number FROM numbers(20000);
INSERT INTO t_soft_limit SELECT number, number FROM numbers(20000, 20000);
"

SETTINGS="--enable_analyzer=1 --make_distributed_plan=1 --distributed_plan_execute_locally=1 \
--serialize_query_plan=1 --distributed_plan_read_in_order=1 --enable_parallel_replicas=0 \
--automatic_parallel_replicas_mode=0 --optimize_read_in_order=1 --max_threads=4"

QUERY_ID="05229_soft_limit_${CLICKHOUSE_DATABASE}"

# `value % 2 = 0` is not indexable, so it becomes a filter above the read: the reader's own limit is
# zeroed, the query keeps its `LIMIT 10`, and the threshold is the only thing that can carry the
# coordinator's task-sizing decision to the worker.
$CLICKHOUSE_CLIENT ${SETTINGS} --query_id "$QUERY_ID" \
    -q "SELECT key FROM t_soft_limit WHERE value % 2 = 0 ORDER BY key LIMIT 10 FORMAT Null"

# The bound is deliberately loose (the whole table is 40000 rows in two parts, and a single stream
# reads a whole part per task without the threshold): it only has to separate single-range first
# tasks from whole-range ones.
$CLICKHOUSE_CLIENT -n -q "
SYSTEM FLUSH LOGS query_log;
SELECT sum(read_rows) < 100000 FROM system.query_log
WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'QueryFinish'
  AND event_date >= yesterday();
"

# The answer must not depend on the task sizing.
$CLICKHOUSE_CLIENT ${SETTINGS} -q "
SELECT count(), sum(key) FROM (SELECT key FROM t_soft_limit WHERE value % 2 = 0 ORDER BY key LIMIT 10);
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_soft_limit;"
