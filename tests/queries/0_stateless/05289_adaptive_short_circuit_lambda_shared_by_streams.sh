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
