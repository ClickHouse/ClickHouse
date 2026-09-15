#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# WASM UDFs over the sorting key can be moved to PREWHERE under FINAL only when declared DETERMINISTIC,
# both by the plan-level optimization and by the legacy AST-level one

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS wasm_prewhere_det;
DROP FUNCTION IF EXISTS wasm_prewhere_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_prewhere_final_test';
EOF

cat "${CUR_DIR}/wasm/identity_int.wasm" | ${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'wasm_prewhere_final_test', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << 'EOF'
CREATE OR REPLACE FUNCTION wasm_prewhere_det
    LANGUAGE WASM FROM 'wasm_prewhere_final_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

CREATE OR REPLACE FUNCTION wasm_prewhere_nondet
    LANGUAGE WASM FROM 'wasm_prewhere_final_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

DROP TABLE IF EXISTS t_wasm_prewhere_final;
CREATE TABLE t_wasm_prewhere_final (k Int32, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
INSERT INTO t_wasm_prewhere_final SELECT number, 'x', 1 FROM numbers(1000);
EOF

MOVE_SETTINGS="--optimize_move_to_prewhere=1 --optimize_move_to_prewhere_if_final=1"

echo "= plan-level optimization: deterministic is moved, non-deterministic is not ="
${CLICKHOUSE_CLIENT} ${MOVE_SETTINGS} --enable_analyzer=1 --query_plan_optimize_prewhere=1 -q "SELECT count() > 0 FROM (EXPLAIN actions=1 SELECT * FROM t_wasm_prewhere_final FINAL WHERE wasm_prewhere_det(k) > 100) WHERE explain LIKE '%Prewhere filter%'"
${CLICKHOUSE_CLIENT} ${MOVE_SETTINGS} --enable_analyzer=1 --query_plan_optimize_prewhere=1 -q "SELECT count() FROM (EXPLAIN actions=1 SELECT * FROM t_wasm_prewhere_final FINAL WHERE wasm_prewhere_nondet(k) > 100) WHERE explain LIKE '%Prewhere filter%'"

echo "= legacy AST-level optimization: deterministic is moved, non-deterministic is not ="
# capture the output first so a failing query breaks the reference instead of counting as 0
count_ast_prewhere() {
    local output
    if ! output=$(${CLICKHOUSE_CLIENT} ${MOVE_SETTINGS} --enable_analyzer=0 --query_plan_optimize_prewhere=0 -q "EXPLAIN SYNTAX SELECT * FROM t_wasm_prewhere_final FINAL WHERE $1" 2>&1); then
        echo "query failed: ${output}"
        return
    fi
    echo "${output}" | grep -c "PREWHERE" || true
}
count_ast_prewhere "wasm_prewhere_det(k) > 100"
count_ast_prewhere "wasm_prewhere_nondet(k) > 100"

${CLICKHOUSE_CLIENT} << 'EOF'
DROP TABLE t_wasm_prewhere_final;
DROP FUNCTION wasm_prewhere_det;
DROP FUNCTION wasm_prewhere_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'wasm_prewhere_final_test';
EOF
