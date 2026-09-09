#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `enable_adaptive_short_circuit_lazy_execution` revisits a decision only after a whole round of rows was
# profiled, so an adaptive `ExpressionActions` instance must survive many blocks to change anything at all.
# The paths which execute an expression once per block from a thread that is not known in advance - a
# `Parquet` row subgroup, a hash join probe block - lease a pooled instance instead of building a new one
# for every block. `AdaptiveShortCircuitEagerExecutions` counts the actions which the static short-circuit
# schedule would have executed lazily and the heuristic decided to execute eagerly instead, so it can only
# be non-zero if the profile of the earlier blocks was preserved.

SETTINGS="SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 1, max_block_size = 8192, max_threads = 1"

function check_profile_is_preserved()
{
    local name="$1"
    local query="$2"
    local query_id="${CLICKHOUSE_DATABASE}_${name}"

    # The result must not depend on the adaptive decisions.
    local adaptive_result
    local static_result
    adaptive_result=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$query $SETTINGS")
    static_result=$(${CLICKHOUSE_CLIENT} --query "$query SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 0, max_block_size = 8192, max_threads = 1")

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            '$name',
            $adaptive_result = $static_result,
            ProfileEvents['AdaptiveShortCircuitEagerExecutions'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE probe (b0 UInt8, b1 UInt8, b2 UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO probe SELECT number % 2, number % 3 != 0, number % 5 != 0 FROM numbers(500000);"

# A filter of the query pipeline: the lazily executed `and` is so cheap that filtering the rows out and
# expanding the result back costs more than executing it on every row.
check_profile_is_preserved "pipeline_filter" "SELECT count() FROM probe WHERE if(b0, and(b1, b2), 0)"

# The residual `JOIN ON` expression of a mixed hash join is executed once per probe block.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE probe_left (k UInt64, b0 UInt8, b1 UInt8) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE probe_right (k UInt64, c0 UInt8) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO probe_left SELECT number % 500, number % 2, number % 3 != 0 FROM numbers(500000);
    INSERT INTO probe_right SELECT number, number % 7 != 0 FROM numbers(500);"

check_profile_is_preserved "hash_join_residual" "
    SELECT count() FROM probe_left AS l INNER JOIN probe_right AS r
    ON l.k = r.k AND if(l.b0, and(l.b1, r.c0), 0)"

# The `PREWHERE` expression of the native `Parquet` reader is executed once per row subgroup.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet', Parquet)
    SELECT number % 2 AS b0, toUInt8(number % 3 != 0) AS b1, toUInt8(number % 5 != 0) AS b2
    FROM numbers(1000000) SETTINGS engine_file_truncate_on_insert = 1"

check_profile_is_preserved "parquet_prewhere" "
    SELECT count() FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet', Parquet) WHERE if(b0, and(b1, b2), 0)"
