#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Check the name of the span produced by `MergeTreeSource::tryGenerate` for every block read from a `MergeTree` table.

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_span_name;
    CREATE TABLE t_span_name (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_span_name SELECT number FROM numbers(10);
"

trace_id=$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(generateUUIDv4()))")

${CLICKHOUSE_CLIENT} --opentelemetry_start_trace_probability=1 --opentelemetry-traceparent "00-${trace_id}-0000000000000010-01" \
    --max_threads=1 --max_block_size=3 \
    -q "SELECT * FROM t_span_name FORMAT Null"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM FLUSH LOGS opentelemetry_span_log;
    SELECT count() > 0
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday()
        AND lower(hex(trace_id)) = '${trace_id}'
        AND startsWith(operation_name, 'MergeTreeSource(' || currentDatabase() || '.t_span_name')
        AND endsWith(operation_name, ')::tryGenerate');
    DROP TABLE t_span_name;
"
