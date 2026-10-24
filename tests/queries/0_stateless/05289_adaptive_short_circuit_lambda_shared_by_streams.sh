#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The body of a lambda is executed by `AdaptiveExpressionActions`, which is stateful, while the prepared
# lambda is shared by every copy of the `ActionsDAG`, i.e. by every pipeline stream, and a lambda folded
# into a constant is shared the same way. A lambda rebuilt from a serialized query plan must honor
# `enable_adaptive_short_circuit_lazy_execution` as well. The results must match the static short-circuit
# schedule in every case; a data race on the shared instance is caught by the sanitizer builds.

ADAPTIVE="short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 1"
STATIC="short_circuit_function_evaluation = 'force_enable', enable_adaptive_short_circuit_lazy_execution = 0"

function compare()
{
    local name="$1"
    local query="$2"
    local settings="$3"
    local adaptive_result
    local static_result
    adaptive_result=$(${CLICKHOUSE_CLIENT} --query "$query SETTINGS $ADAPTIVE, $settings")
    static_result=$(${CLICKHOUSE_CLIENT} --query "$query SETTINGS $STATIC, $settings")
    if [ "$adaptive_result" = "$static_result" ]; then
        echo "$name OK"
    else
        echo "$name: $adaptive_result != $static_result"
    fi
}

STREAMS="max_threads = 8, max_block_size = 1000"

# A lambda with a captured column, executed from many streams.
compare "captured" "
    SELECT sum(arrayCount(x -> and(x % 2 = 0, intDiv(number, x % 3 + 1) % 5 != 0), range(number % 50)))
    FROM numbers_mt(200000)" "$STREAMS"

# A lambda without captured columns is folded into a constant, which is shared by every stream.
compare "constant" "
    SELECT sum(arrayCount(x -> and(x % 2 = 0, intDiv(x, x % 3 + 1) % 5 != 0), range(number % 50)))
    FROM numbers_mt(200000)" "$STREAMS"

# The constant lambda is serialized with the query plan and rebuilt on the remote side.
compare "serialized" "
    SELECT sum(arrayCount(x -> and(x % 2 = 0, intDiv(x, x % 3 + 1) % 5 != 0), range(number % 50)))
    FROM remote('127.0.0.{1,2}', numbers(100000))" "$STREAMS, serialize_query_plan = 1, prefer_localhost_replica = 0"

# A lambda with a captured column is serialized with the query plan as a `FunctionCapture` and rebuilt on
# the remote side. Besides the result, check that the heuristic is really active in the rebuilt lambda body:
# `AdaptiveShortCircuitEagerExecutions` counts the actions which the static schedule would have executed
# lazily and the heuristic executed eagerly, so it is only non-zero if the adaptive schedule was used.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE lambda_probe (b UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO lambda_probe SELECT number % 3 != 0, arrayMap(i -> toUInt8((number + i) % 5 != 0), range(10)) FROM numbers(500000);"

QUERY_ID="${CLICKHOUSE_DATABASE}_serialized_captured"
CAPTURED_QUERY="
    SELECT sum(arrayCount(x -> if(x % 2, and(x, b), 0), arr))
    FROM remote('127.0.0.{1,2}', currentDatabase(), lambda_probe)"
SERIALIZED="serialize_query_plan = 1, prefer_localhost_replica = 0, max_threads = 1, max_block_size = 8192"
adaptive_result=$(${CLICKHOUSE_CLIENT} --query_id "$QUERY_ID" --query "$CAPTURED_QUERY SETTINGS $ADAPTIVE, $SERIALIZED")
static_result=$(${CLICKHOUSE_CLIENT} --query "$CAPTURED_QUERY SETTINGS $STATIC, $SERIALIZED")

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        'serialized_captured',
        $adaptive_result = $static_result,
        sum(ProfileEvents['AdaptiveShortCircuitEagerExecutions']) > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish'
        -- The secondary queries run in the default database, so match them by the initial query.
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND query_id = '$QUERY_ID')"
