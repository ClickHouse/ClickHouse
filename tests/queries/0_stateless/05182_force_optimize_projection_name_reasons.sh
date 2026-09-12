#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_force_reasons;

CREATE TABLE t_force_reasons
(
    key UInt64,
    value UInt64,
    other UInt64,
    PROJECTION p_by_value (SELECT value, other ORDER BY value),
    PROJECTION p_by_other (SELECT value, other ORDER BY other)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_force_reasons SELECT number, number * 2, number % 100 FROM numbers(100000);
"

SETTINGS="--enable_analyzer 1 --optimize_use_projections 1 --enable_parallel_replicas 0 --automatic_parallel_replicas_mode 0"

# Both projections can serve the read. p_by_value is chosen, so the error for the forced p_by_other says why it lost.
$CLICKHOUSE_CLIENT $SETTINGS --force_optimize_projection_name p_by_other \
    -q "SELECT value FROM t_force_reasons WHERE value < 10 AND other < 50" 2>&1 \
    | grep -o 'but not used: [^.]*' | sed 's/[0-9]\+ marks/N marks/g'

# No projection has the required column, so the error lists why each one was rejected.
$CLICKHOUSE_CLIENT $SETTINGS --force_optimize_projection 1 \
    -q "SELECT key FROM t_force_reasons WHERE key < 10" 2>&1 \
    | grep -o 'force_optimize_projection = 1: [^.]*'
