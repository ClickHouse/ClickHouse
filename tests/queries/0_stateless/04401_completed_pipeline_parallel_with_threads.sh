#!/usr/bin/env bash

# PARALLEL WITH combines several completed pipelines into one and runs it through
# CompletedPipelineExecutor. The combined pipeline must run with the sum of the thread
# counts of the combined pipelines, otherwise the executor runs it single-threaded and
# the branches execute sequentially.
#
# Each branch runs with max_threads=1, so with the fix the combined pipeline runs with
# one thread per branch and all branches overlap; without it a single thread walks the
# branches one by one. peak_threads_usage also counts the executor's driver/polling
# thread, so the single-threaded case already reports ~2; five branches give a margin
# well above that. sleepEachRow keeps all branches alive at the same time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_pw_threads_1;
DROP TABLE IF EXISTS t_pw_threads_2;
DROP TABLE IF EXISTS t_pw_threads_3;
DROP TABLE IF EXISTS t_pw_threads_4;
DROP TABLE IF EXISTS t_pw_threads_5;
CREATE TABLE t_pw_threads_1 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_pw_threads_2 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_pw_threads_3 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_pw_threads_4 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_pw_threads_5 (x UInt64) ENGINE = MergeTree ORDER BY x;
"

QUERY_ID="04401_pw_threads_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --max_threads=1 --use_concurrency_control=0 -q "
INSERT INTO t_pw_threads_1 SELECT sleepEachRow(0.1) + number FROM numbers(2)
PARALLEL WITH
INSERT INTO t_pw_threads_2 SELECT sleepEachRow(0.1) + number FROM numbers(2)
PARALLEL WITH
INSERT INTO t_pw_threads_3 SELECT sleepEachRow(0.1) + number FROM numbers(2)
PARALLEL WITH
INSERT INTO t_pw_threads_4 SELECT sleepEachRow(0.1) + number FROM numbers(2)
PARALLEL WITH
INSERT INTO t_pw_threads_5 SELECT sleepEachRow(0.1) + number FROM numbers(2)
"

$CLICKHOUSE_CLIENT -q "
SELECT count() FROM merge(currentDatabase(), '^t_pw_threads_')
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT peak_threads_usage > 3
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$QUERY_ID'
"

$CLICKHOUSE_CLIENT -q "
DROP TABLE t_pw_threads_1;
DROP TABLE t_pw_threads_2;
DROP TABLE t_pw_threads_3;
DROP TABLE t_pw_threads_4;
DROP TABLE t_pw_threads_5;
"
