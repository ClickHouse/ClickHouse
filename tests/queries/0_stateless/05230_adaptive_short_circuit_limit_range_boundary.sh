#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The boundary conditions of `LIMIT ... AFTER` and `LIMIT ... UNTIL` are evaluated by one
# `LimitRangeTransform` on a single stream, which is reused for every chunk until the range is over.
# That is the shape the adaptive heuristic needs, so the boundary evaluators are built with
# `ExpressionActions::create` and honor `enable_adaptive_short_circuit_lazy_execution`.
# `AdaptiveShortCircuitEagerExecutions` counts the actions which the static short-circuit schedule
# would have executed lazily and the heuristic decided to execute eagerly instead.

SETTINGS="SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 1, max_block_size = 8192, max_threads = 1"
STATIC_SETTINGS="SETTINGS short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 0, max_block_size = 8192, max_threads = 1"

function check_profile_is_preserved()
{
    local name="$1"
    local query="$2"
    local query_id="${CLICKHOUSE_DATABASE}_${name}"

    # The result must not depend on the adaptive decisions.
    local adaptive_result
    local static_result
    adaptive_result=$(${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "$query $SETTINGS")
    static_result=$(${CLICKHOUSE_CLIENT} --query "$query $STATIC_SETTINGS")

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT
            '$name',
            $adaptive_result = $static_result,
            ProfileEvents['AdaptiveShortCircuitEagerExecutions'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
}

# The boundary is reached only near the end of the table, so the condition is evaluated over enough
# rows for the heuristic to profile it and revisit its decision. `ORDER BY n` pins the row order:
# without it, the range depends on the read order, which is not deterministic with parallel replicas.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE boundary (n UInt64, b0 UInt8, b1 UInt8, b2 UInt8) ENGINE = MergeTree ORDER BY n;
    INSERT INTO boundary SELECT number, number % 2, number > 400000, number % 5 != 0 FROM numbers(500000);"

check_profile_is_preserved "limit_after" "
    SELECT count() FROM (SELECT n FROM boundary ORDER BY n LIMIT 100 AFTER if(b0, and(b1, b2), 0))"

check_profile_is_preserved "limit_until" "
    SELECT count() FROM (SELECT n FROM boundary ORDER BY n LIMIT 1000000 UNTIL if(b0, and(b1, b2), 0))"
