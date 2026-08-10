#!/usr/bin/env bash
# Tests that a WebAssembly UDF declared without DETERMINISTIC is not merged into a JOIN condition,
# while its DETERMINISTIC twin still is.

# Tags: no-fasttest, no-msan
# no-fasttest: the fast build has no WebAssembly engine.
# no-msan: WebAssembly UDFs are not run under MSan, like every other wasm test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE="mod_${CLICKHOUSE_DATABASE}"
FN_ND="wasm_nd_${CLICKHOUSE_DATABASE}"
FN_D="wasm_d_${CLICKHOUSE_DATABASE}"

# CREATE FUNCTION is server-global, so drop both UDFs on every exit path.
cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_ND}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_D}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'" 2>/dev/null
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -q "INSERT INTO system.webassembly_modules (name, code)
    SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}"/wasm/host_api.wasm

${CLICKHOUSE_CLIENT} <<EOF
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET explain_query_plan_default = 'legacy';

CREATE OR REPLACE FUNCTION ${FN_ND} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random'
    ARGUMENTS (UInt32) RETURNS UInt32;
CREATE OR REPLACE FUNCTION ${FN_D} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random'
    ARGUMENTS (UInt32) RETURNS UInt32 DETERMINISTIC;

CREATE TABLE lw (k UInt32, a UInt32) ENGINE = Memory;
CREATE TABLE rw (k UInt32, b UInt8) ENGINE = Memory;
INSERT INTO lw SELECT number % 4, number FROM numbers(400);
INSERT INTO rw SELECT number, number FROM numbers(4);

SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT rw.b FROM lw JOIN rw ON lw.k = rw.k
    WHERE toUInt8(${FN_ND}(toUInt32(lw.a)) % 4) = rw.b);

SELECT countIf(explain LIKE '%Clauses: [(__table1.k, %') FROM (
    EXPLAIN PLAN actions = 1 SELECT rw.b FROM lw JOIN rw ON lw.k = rw.k
    WHERE toUInt8(${FN_D}(toUInt32(lw.a)) % 4) = rw.b);

DROP TABLE lw;
DROP TABLE rw;
EOF
