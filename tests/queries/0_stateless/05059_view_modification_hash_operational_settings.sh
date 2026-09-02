#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# A view's modification hash folds the effective reader's settings, because they can change the rows
# the view returns. Settings that only drive diagnostics - query logging, profiling, tracing - cannot,
# and a definer's settings profile routinely carries them. Folding them in would turn an unrelated
# profile edit into a data change: a `REFRESH ... IF CHANGED APPEND` view would then discard its
# watermark and append a duplicate copy of unchanged rows.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_hash_ops_05059 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_hash_ops_05059 VALUES (1);
    CREATE VIEW v_hash_ops_05059 AS SELECT x FROM t_hash_ops_05059;
"

hash_of_view()
{
    $CLICKHOUSE_CLIENT "$@" -q "
        SELECT toString(modification_hash)
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'v_hash_ops_05059'
    "
}

baseline=$(hash_of_view)
[ -n "${baseline}" ] && echo 'hash is computed'

logging=$(hash_of_view --log_queries 1 --log_query_threads 1 --log_profile_events 1 --log_queries_min_query_duration_ms 100)
[ "${baseline}" = "${logging}" ] && echo 'query logging settings do not change the hash'

profiling=$(hash_of_view --query_profiler_real_time_period_ns 1000000 --memory_profiler_step 4194304 --trace_profile_events 1)
[ "${baseline}" = "${profiling}" ] && echo 'profiler settings do not change the hash'

tracing=$(hash_of_view --opentelemetry_start_trace_probability 1 --opentelemetry_trace_processors 1)
[ "${baseline}" = "${tracing}" ] && echo 'tracing settings do not change the hash'

# A setting that can change the rows the view returns still has to move the hash.
policy=$(hash_of_view --additional_table_filters "{'t_hash_ops_05059': 'x != 1'}")
[ "${baseline}" != "${policy}" ] && echo 'a row filter changes the hash'

# And so does a real change of the source.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_hash_ops_05059 VALUES (2)"
[ "${baseline}" != "$(hash_of_view)" ] && echo 'an insert into the source changes the hash'

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v_hash_ops_05059;
    DROP TABLE t_hash_ops_05059;
"
