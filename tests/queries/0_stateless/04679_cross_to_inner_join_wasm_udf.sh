#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the fast build has no WebAssembly engine.
# no-msan: WebAssembly UDFs are not run under MSan, like every other wasm test.
# The module and function names are derived from CLICKHOUSE_DATABASE, so parallel copies of this test
# never collide and no-parallel is not needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE="mod_${CLICKHOUSE_DATABASE}"
FN_ND="wasm_nd_${CLICKHOUSE_DATABASE}"
FN_D="wasm_d_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_ND}"
${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_D}"
${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"

${CLICKHOUSE_CLIENT} -q "INSERT INTO system.webassembly_modules (name, code)
    SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}"/wasm/buffered_abi.wasm

# A WebAssembly UDF declared without DETERMINISTIC may return a different value for the same input, so
# it must not become a join key. It is refused through `isDeterministicInScopeOfQuery`, which the UDF
# reports from its own declaration. It is not constant-folded, so it reaches the pass as an ordinary
# function node. The DETERMINISTIC twin below is the control: it must stay eligible, so the refusal
# keys on the declaration and not merely on the function being a UDF.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET enable_analyzer = 1;
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS r SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE l (a UInt64) ENGINE = Log;
CREATE TABLE r (a UInt64) ENGINE = Log;
INSERT INTO l SELECT number % 16 FROM numbers(100);
INSERT INTO r SELECT number % 16 FROM numbers(100);

CREATE OR REPLACE FUNCTION ${FN_ND} LANGUAGE WASM ABI BUFFERED_V1 FROM '${MODULE}' :: 'get_block_size'
    ARGUMENTS (value UInt64) RETURNS UInt64 SETTINGS serialization_format = 'CSV';
CREATE OR REPLACE FUNCTION ${FN_D} LANGUAGE WASM ABI BUFFERED_V1 FROM '${MODULE}' :: 'get_block_size'
    ARGUMENTS (value UInt64) RETURNS UInt64 DETERMINISTIC SETTINGS serialization_format = 'CSV';

SELECT '-- a non-deterministic WebAssembly UDF is no longer rewritten';
SELECT count() = 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + ${FN_ND}(l.a) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

SELECT '-- a DETERMINISTIC WebAssembly UDF is still rewritten';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE run_passes = 1
    SELECT count() FROM l, r WHERE l.a + ${FN_D}(l.a) = r.a
    SETTINGS cross_to_inner_join_rewrite = 1
) WHERE explain ILIKE '%kind: INNER%';

DROP FUNCTION ${FN_ND};
DROP FUNCTION ${FN_D};
DROP TABLE l SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE r SETTINGS ignore_drop_queries_probability = 0;
EOF

${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
