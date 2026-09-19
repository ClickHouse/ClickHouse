#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# The legacy predicate push-down (`PredicateExpressionsOptimizer`) duplicates an outer `WHERE`
# predicate into the subquery while keeping the outer copy. `ExpressionInfoVisitor` must resolve
# user-defined functions through their own factories, otherwise a WASM UDF that is not declared
# `DETERMINISTIC` (or an `EXECUTABLE` UDF) looks like an unknown, and therefore deterministic,
# function, gets evaluated twice per row and may silently drop rows. A UDF declared `DETERMINISTIC`
# is still pushed down.
#
# The analyzer can no longer be disabled by a query, but `EXPLAIN AST optimize = 1` still runs the
# old interpreter, so it is the entry point into the legacy optimizer. One occurrence of the
# function in the dumped AST means the predicate stayed in the outer query, two mean it was copied
# into the subquery as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'SQL'
DROP FUNCTION IF EXISTS wasm_legacy_pd_det;
DROP FUNCTION IF EXISTS wasm_legacy_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_legacy_pd_test';
SQL

${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_legacy_pd_test', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} << 'SQL'
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

${CLICKHOUSE_CLIENT} --enable_optimize_predicate_expression=1 --webassembly_udf_max_fuel=100000000 << 'SQL'
-- The predicate stays in the outer query only.
SELECT 'nondeterministic', countIf(explain LIKE '%Function wasm_legacy_pd_nondet%')
FROM (EXPLAIN AST optimize = 1 SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_nondet(k) % 2 = 0);
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_nondet(k) % 2 = 0;

-- The predicate is duplicated into the subquery.
SELECT 'deterministic', countIf(explain LIKE '%Function wasm_legacy_pd_det%')
FROM (EXPLAIN AST optimize = 1 SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_det(k) % 2 = 0);
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE wasm_legacy_pd_det(k) % 2 = 0;

-- An `EXECUTABLE` UDF (`test_function` comes from `tests/config/test_function.xml`) is never deterministic
-- in the scope of a query, so its predicate stays in the outer query as well.
SELECT 'executable', countIf(explain LIKE '%Function test_function%')
FROM (EXPLAIN AST optimize = 1 SELECT k FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function(toUInt64(k), toUInt64(k)) % 2 = 0);
SELECT count() FROM (SELECT k FROM t_wasm_legacy_pd) WHERE test_function(toUInt64(k), toUInt64(k)) % 2 = 0;

-- The same `EXECUTABLE` UDF in the subquery `SELECT` list goes through `hasNonRewritableFunction`.
-- The walk must complete. The outer predicate is written over the alias `v`, which is not expanded
-- before the determinism check, so this one is still pushed down - a separate, pre-existing gap.
SELECT 'executable in subquery', countIf(explain LIKE '%Function test_function%')
FROM (EXPLAIN AST optimize = 1 SELECT v FROM (SELECT test_function(toUInt64(k), toUInt64(k)) AS v FROM t_wasm_legacy_pd) WHERE v % 2 = 0);
SQL

${CLICKHOUSE_CLIENT} << 'SQL'
DROP TABLE t_wasm_legacy_pd;
DROP FUNCTION wasm_legacy_pd_det;
DROP FUNCTION wasm_legacy_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_legacy_pd_test';
SQL
