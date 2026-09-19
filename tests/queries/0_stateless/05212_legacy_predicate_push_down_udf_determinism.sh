#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# The legacy (`enable_analyzer = 0`) predicate push-down duplicates an outer `WHERE` predicate into the
# subquery while keeping the outer copy. `ExpressionInfoVisitor` must resolve user-defined functions
# through their own factories, otherwise a WASM UDF that is not declared `DETERMINISTIC` (or an
# `EXECUTABLE` UDF) looks like an unknown, and therefore deterministic, function, gets evaluated twice
# per row and may silently drop rows. A UDF declared `DETERMINISTIC` is still pushed down.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << 'SQL'
DROP FUNCTION IF EXISTS wasm_legacy_pd_det;
DROP FUNCTION IF EXISTS wasm_legacy_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_legacy_pd_test';
SQL

${CLICKHOUSE_CLIENT} --enable_analyzer=1 \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_legacy_pd_test', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << 'SQL'
CREATE OR REPLACE FUNCTION wasm_legacy_pd_nondet
    LANGUAGE WASM FROM 'identity_legacy_pd_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION wasm_legacy_pd_det
    LANGUAGE WASM FROM 'identity_legacy_pd_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

DROP TABLE IF EXISTS t_wasm_legacy_pd;
CREATE TABLE t_wasm_legacy_pd (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_wasm_legacy_pd SELECT number FROM numbers(10);
SQL

${CLICKHOUSE_CLIENT} --enable_analyzer=0 --enable_optimize_predicate_expression=1 --webassembly_udf_max_fuel=100000000 << 'SQL'
-- The predicate stays in the outer query only.
SELECT 'nondeterministic';
EXPLAIN SYNTAX SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_nondet(k) % 2 = 0 FORMAT TSVRaw;
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_nondet(k) % 2 = 0;

-- The predicate is duplicated into the subquery.
SELECT 'deterministic';
EXPLAIN SYNTAX SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_det(k) % 2 = 0 FORMAT TSVRaw;
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_det(k) % 2 = 0;

-- An `EXECUTABLE` UDF (`test_function` comes from `tests/config/test_function.xml`) is never deterministic
-- in the scope of a query, so its predicate stays in the outer query as well.
SELECT 'executable';
EXPLAIN SYNTAX SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function(toUInt64(k), toUInt64(k)) % 2 = 0 FORMAT TSVRaw;
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function(toUInt64(k), toUInt64(k)) % 2 = 0;

-- A parametric `EXECUTABLE` UDF must be recognized by name only: instantiating it with an empty
-- `parameters` array would throw `BAD_ARGUMENTS` from inside the optimizer walk.
SELECT 'executable with parameter';
EXPLAIN SYNTAX SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function_with_parameter(2)(toUInt64(k)) % 2 = 0 FORMAT TSVRaw;
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function_with_parameter(2)(toUInt64(k)) % 2 = 0;

-- The same UDF in the subquery `SELECT` list goes through `hasNonRewritableFunction`.
SELECT 'executable with parameter in subquery';
SELECT count() FROM (SELECT test_function_with_parameter(2)(toUInt64(k)) AS v FROM t_wasm_legacy_pd) WHERE v % 2 = 0;
SQL

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << 'SQL'
DROP TABLE t_wasm_legacy_pd;
DROP FUNCTION wasm_legacy_pd_det;
DROP FUNCTION wasm_legacy_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_legacy_pd_test';
SQL
