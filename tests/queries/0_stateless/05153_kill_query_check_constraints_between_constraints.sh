#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that KILL QUERY landing between two `CHECK` constraints of the same table stops the insert
# before the next constraint expression starts. `ConstraintsDescription::getExpressions` builds one
# `ExpressionActions` per constraint, and `ExpressionActions::execute` polls the cancellation flag
# only after its first action, so a cancelled insert must not enter the next `execute` at all.
# no-parallel: check_constraints_transform_between_constraints_pause is a global PAUSEABLE failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_check_constraints_between_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_check_constraints_between_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_between_constraints_pause" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_check_constraints_between" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05153" 2>/dev/null;
      ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '\''faulty_05153'\''" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_between_constraints_pause"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS kill_query_check_constraints_between"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS infinite_loop_05153"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05153'"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'faulty_05153', code FROM input('code String') FORMAT RawBlob" < "${CUR_DIR}"/wasm/faulty.wasm

${CLICKHOUSE_CLIENT} -q "
    CREATE OR REPLACE FUNCTION infinite_loop_05153 LANGUAGE WASM ABI ROW_DIRECT FROM 'faulty_05153' :: 'infinite_loop_signal' ARGUMENTS (UInt32) RETURNS UInt32;
"

# The first constraint is cheap and the second one starts a function that never returns on its own,
# so the insert can only finish if the cancellation between the two constraints is missed.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE kill_query_check_constraints_between
    (
        x UInt32,
        CONSTRAINT c_cheap CHECK x > 0,
        CONSTRAINT c_slow CHECK infinite_loop_05153(x) = 0
    ) ENGINE = Memory
"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT check_constraints_transform_between_constraints_pause"

timeout 120 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    INSERT INTO kill_query_check_constraints_between
    SELECT toUInt32(1)
    SETTINGS webassembly_udf_max_fuel = 0, max_threads = 1
" >"$output_file" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT check_constraints_transform_between_constraints_pause PAUSE"
then
    echo "FAIL: timed out waiting for the between-constraints failpoint — the first constraint did not finish"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_between_constraints_pause"
    exit 1
fi

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id' ASYNC" >/dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT check_constraints_transform_between_constraints_pause"

wait

grep -qE "QUERY_WAS_CANCELLED|WASM_ERROR" "$output_file" || { echo "FAIL: the insert was not cancelled between the two constraints"; cat "$output_file"; exit 1; }

${CLICKHOUSE_CLIENT} -q "DROP TABLE kill_query_check_constraints_between"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION infinite_loop_05153"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = 'faulty_05153'"

echo "OK"
