#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: JIT compilation is not available in fasttest

# `intDiv`, `modulo` and `positiveModulo` are compiled with errors reported to the caller.
# Compiled and interpreted execution must give the same results and fail with the same error code and message.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JIT="--compile_expressions=1 --min_count_to_compile_expression=0"
NO_JIT="--compile_expressions=0"

# Prints the result of a query, or "Code N: message" of its exception without the context added by the executor.
function outcome()
{
    local settings=$1
    local query=$2
    local result
    # shellcheck disable=SC2086
    result=$($CLICKHOUSE_CLIENT $settings --query "$query" 2>&1)
    if echo "$result" | grep -q "^Code: "; then
        echo "$result" | grep "^Code: " | sed -E 's/^Code: ([0-9]+)\..*DB::Exception: ([^:(]*[^:( ]).*/Code \1: \2/'
    else
        echo "$result"
    fi
}

# Runs a query with and without the JIT and prints the outcome if both agree.
function compare()
{
    local name=$1
    local query=$2
    local extra=${3:-}
    local compiled interpreted
    compiled=$(outcome "$JIT $extra" "$query")
    interpreted=$(outcome "$NO_JIT $extra" "$query")
    if [ "$compiled" == "$interpreted" ]; then
        echo "$name: $compiled"
    else
        echo "$name: DIFFERENT"
        echo "  compiled: $compiled"
        echo "  interpreted: $interpreted"
    fi
}

TYPES=(Int8 UInt8 Int16 UInt16 Int32 UInt32 Int64 UInt64)
MINIMUMS=(-128 0 -32768 0 -2147483648 0 -9223372036854775808 0)
MAXIMUMS=(127 255 32767 65535 2147483647 4294967295 9223372036854775807 18446744073709551615)

# One column per type. Each row takes its value from an edge set (minimum, maximum, -1, 0, 1, 2, -2) or from a hash.
# The columns use three different selectors, so that pairs of columns meet in all combinations of edge values.
create="CREATE TABLE jit_division (k UInt32"
fill="SELECT number"
for j in "${!TYPES[@]}"; do
    type=${TYPES[$j]}
    selector="intDiv(number, $((11 ** (j % 3)))) % 11"
    create+=", v$j $type"
    fill+=", multiIf($selector = 0, to$type('${MINIMUMS[$j]}'), $selector = 1, to$type('${MAXIMUMS[$j]}'),
        $selector = 2, to$type(-1), $selector = 3, to$type(0), $selector = 4, to$type(1), $selector = 5, to$type(2),
        $selector = 6, to$type(-2), to$type(cityHash64(number, $j)))"
done
create+=") ENGINE = MergeTree ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_division"
$CLICKHOUSE_CLIENT --query "$create"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_division $fill FROM numbers(4000)"

# All pairs of types. The divisor is replaced by 1 on the rows where the function throws (where its `OrNull` variant
# returns NULL; for `positiveModulo` where `moduloOrNull` does, or where a signed divisor is its minimum), so the query does not fail.
# Each function is fused with `plus`, so that it is compiled.
for function in intDiv modulo positiveModulo; do
    sums=""
    for a in "${!TYPES[@]}"; do
        for b in "${!TYPES[@]}"; do
            safe="if(${function}OrNull(v$a, v$b) IS NULL, 1, v$b)"
            if [ "$function" == "positiveModulo" ]; then
                # `positiveModuloOrNull` throws on the most negative divisor too, so detect the FPE cases with `moduloOrNull`.
                safe="if(moduloOrNull(v$a, v$b) IS NULL, 1, v$b)"
                if [ "${MINIMUMS[$b]}" != "0" ]; then
                    safe="if(moduloOrNull(v$a, v$b) IS NULL OR v$b = to${TYPES[$b]}('${MINIMUMS[$b]}'), 1, v$b)"
                fi
            fi
            sums+="sum(cityHash64($function(v$a, $safe) + 1)), "
        done
    done
    compare "$function, all type pairs" "SELECT cityHash64(${sums%, }) FROM jit_division"
done

# The compiled query ran compiled.
$CLICKHOUSE_CLIENT $JIT --log_comment "jit_division_compiled_$CLICKHOUSE_DATABASE" \
    --query "SELECT sum(intDiv(v6, if(v4 = 0 OR v4 = -1, 7, v4)) + 1) FROM jit_division FORMAT Null"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT 'compiled', ProfileEvents['CompiledFunctionRows'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'jit_division_compiled_$CLICKHOUSE_DATABASE'
        AND event_date >= yesterday()"

# Errors. A small table read in order by one thread.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_division_rows"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_division_rows (k UInt32, a Int32, b Int32, n Nullable(Int32), nb Nullable(Int32)) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_division_rows VALUES
    (0, 10, 3, 1, 3), (1, -2147483648, -1, 2, 5), (2, 7, 2, NULL, 0), (3, 9, 0, 4, NULL), (4, 5, 5, NULL, 0)"

compare "division by zero" "SELECT sum(intDiv(a, b) + 1) FROM jit_division_rows WHERE k >= 3" "--max_threads=1"
compare "minimal signed number by minus one" "SELECT sum(intDiv(a, b) + 1) FROM jit_division_rows WHERE k <= 2" "--max_threads=1"
compare "first failing row wins (minimal, then zero)" "SELECT sum(intDiv(a, b) + 1) FROM jit_division_rows" "--max_threads=1"
compare "first failing row wins (zero, then minimal)" \
    "SELECT sum(intDiv(a, b) + 1) FROM (SELECT a, b FROM jit_division_rows ORDER BY k DESC LIMIT 100)" "--max_threads=1"
compare "modulo by zero" "SELECT sum(modulo(a, b) + 1) FROM jit_division_rows WHERE k >= 3" "--max_threads=1"
compare "modulo minimal by minus one" "SELECT sum(modulo(a, b) + 1) FROM jit_division_rows WHERE k = 1" "--max_threads=1"
compare "positiveModulo by the most negative number" \
    "SELECT sum(positiveModulo(toInt8(a % 100), toInt8(-128)) + 1) FROM jit_division_rows WHERE k = 0 OR k = 1" "--max_threads=1"
compare "positiveModulo, the most negative number with a non-negative remainder" \
    "SELECT sum(positiveModulo(toInt8(a % 100), toInt8(-128)) + 1) FROM jit_division_rows WHERE k = 0 OR k = 2" "--max_threads=1"

# NULLs. With a non-Nullable divisor, a NULL argument makes the row NULL and it does not fail. With a Nullable divisor,
# only a NULL divisor does: a NULL dividend with a zero divisor fails, as in the interpreter.
compare "NULL dividend, zero divisor" "SELECT groupArray(intDiv(n, if(k = 2, 0, b)) + 1) FROM jit_division_rows WHERE k = 0 OR k = 2" "--max_threads=1"
compare "NULL divisor" "SELECT groupArray(intDiv(a, nb) + 1) FROM jit_division_rows WHERE k = 0 OR k = 3" "--max_threads=1"
compare "NULL dividend, Nullable zero divisor" "SELECT groupArray(intDiv(n, nb) + 1) FROM jit_division_rows WHERE k = 4" "--max_threads=1"
compare "Nullable zero divisor" "SELECT groupArray(intDiv(a, nb) + 1) FROM jit_division_rows WHERE k = 2" "--max_threads=1"
compare "LowCardinality(Nullable) divisor" \
    "SELECT groupArray(intDiv(a, toLowCardinality(nb)) + 1) FROM jit_division_rows WHERE k = 0 OR k = 3" "--max_threads=1"

# Short-circuit evaluation: a guarded division does not fail; without short-circuit evaluation both branches run and fail.
# A separate table without the minimal-by-minus-one row, so that the result does not depend on the order of the conditions.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_division_guard"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_division_guard ENGINE = Memory AS SELECT * FROM jit_division_rows WHERE k != 1"
compare "guard in if, short-circuit" "SELECT sum(if(b != 0, intDiv(a, b) + 1, 0)) FROM jit_division_guard" \
    "--max_threads=1 --short_circuit_function_evaluation=enable"
compare "guard in if, no short-circuit" "SELECT sum(if(b != 0, intDiv(a, b) + 1, 0)) FROM jit_division_guard" \
    "--max_threads=1 --short_circuit_function_evaluation=disable"
compare "guard in and, short-circuit" "SELECT count() FROM jit_division_guard WHERE b != 0 AND intDiv(a, b) + 1 > 2" \
    "--max_threads=1 --short_circuit_function_evaluation=enable"

# PREWHERE and row-level filters are compiled too.
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS jit_division_mt"
$CLICKHOUSE_CLIENT --query "CREATE TABLE jit_division_mt (k UInt32, a Int32, b Int32) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --query "INSERT INTO jit_division_mt SELECT number, number * 7 - 100, number % 5 FROM numbers(1000)"
compare "PREWHERE division by zero" "SELECT count() FROM jit_division_mt PREWHERE intDiv(a, b) + 1 > 3"
compare "PREWHERE guarded division" "SELECT count() FROM jit_division_mt PREWHERE b != 0 AND intDiv(a, b) + 1 > 3"
compare "PREWHERE modulo" "SELECT count() FROM jit_division_mt PREWHERE b != 0 AND modulo(a, b) + 1 > 2"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_division"
$CLICKHOUSE_CLIENT --query "DROP TABLE jit_division_rows"
$CLICKHOUSE_CLIENT --query "DROP TABLE jit_division_guard"
$CLICKHOUSE_CLIENT --query "DROP TABLE jit_division_mt"
