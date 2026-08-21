#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY interrupts a WASM UDF running in a DEFAULT expression of a column
# omitted from the inserted data. For INSERT ... FORMAT, such defaults are evaluated by
# AddingDefaultsTransform, so this covers AddingDefaultsTransform::onCancel -> cancelExecution
# while the guest is executing.
# no-parallel: wasm_guest_pause is a global PAUSEABLE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_adding_defaults_wasm_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_adding_defaults_wasm_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_wasm" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05029" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05029'\''" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_adding_defaults_wasm"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05029"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05029'"

cat "${CUR_DIR}"/wasm/faulty.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05029', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05029 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05029' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE kill_query_adding_defaults_wasm
    (
        x UInt32,
        y UInt32 DEFAULT infinite_loop_05029(x)
    ) ENGINE = Memory
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT wasm_guest_pause"

# The column y is omitted from the data, so AddingDefaultsTransform evaluates its DEFAULT.
# The INSERT goes over HTTP so that the data is parsed on the server in the main insert pipeline
# (getSourceFromASTInsertQuery): clickhouse-client parses inline data on the client side, and
# async_insert = 1 (the default) would move the evaluation to a detached flush thread of
# AsynchronousInsertQueue that KILL QUERY cannot reach.
timeout 60 ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${query_id}&async_insert=0&webassembly_udf_max_fuel=0&max_threads=1&input_format_defaults_for_omitted_fields=1" \
    --data-binary 'INSERT INTO kill_query_adding_defaults_wasm FORMAT JSONEachRow {"x":1}' >"$output_file" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT wasm_guest_pause PAUSE"
then
    echo "FAIL: timed out waiting for the wasm_guest_pause failpoint — the DEFAULT expression did not start executing"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
    exit 1
fi

# onCancel forwards cancellation into the function currently running in AddingDefaultsTransform.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT wasm_guest_pause"

wait

grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_adding_defaults_wasm"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION infinite_loop_05029"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05029'"

echo "OK"
