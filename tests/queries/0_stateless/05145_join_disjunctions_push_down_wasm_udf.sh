#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# A WASM UDF not declared `DETERMINISTIC` must not have its call cloned below a join by
# `use_join_disjunctions_push_down`: the pre-filter and the filter above the join would evaluate it
# independently, so a row accepted by the query's own filter could be dropped by the pre-filter.
# A UDF declared `DETERMINISTIC` is deterministic in the scope of a query, so it may be extracted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << 'SQL'
DROP FUNCTION IF EXISTS wasm_disj_det;
DROP FUNCTION IF EXISTS wasm_disj_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_disj_test';
SQL

${CLICKHOUSE_CLIENT} --enable_analyzer=1 \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_disj_test', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} --enable_analyzer=1 << 'SQL'
SET webassembly_udf_max_fuel = 100000000;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET use_join_disjunctions_push_down = 1;
-- The plan assertions below sort the matched lines: the order in which the plan prints the filter
-- above the join, the pre-filters below it and the `PREWHERE` steps depends on which side the join
-- is read from, which the settings randomizer is free to flip.

CREATE OR REPLACE FUNCTION wasm_disj_nondet
    LANGUAGE WASM FROM 'identity_disj_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION wasm_disj_det
    LANGUAGE WASM FROM 'identity_disj_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

DROP TABLE IF EXISTS t_wasm_disj_left;
DROP TABLE IF EXISTS t_wasm_disj_right;
CREATE TABLE t_wasm_disj_left (a Int32, k Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_wasm_disj_right (k Int32, x Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_wasm_disj_left SELECT 1, number FROM numbers(1000);
INSERT INTO t_wasm_disj_right SELECT number, 100 FROM numbers(1000);

-- The deterministic parts of the disjunction are extracted into a pre-filter per side, but neither
-- pre-filter mentions the non-deterministic UDF.
SELECT 'nondeterministic plan';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_wasm_disj_left JOIN t_wasm_disj_right ON t_wasm_disj_left.k = t_wasm_disj_right.k
    WHERE (t_wasm_disj_left.a = 1 AND t_wasm_disj_right.x = 100 AND wasm_disj_nondet(t_wasm_disj_left.k) % 2 = 0)
       OR (t_wasm_disj_left.a = 2 AND t_wasm_disj_right.x = 200)
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

-- The `DETERMINISTIC` UDF is extracted together with the rest of its branch.
SELECT 'deterministic plan';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_wasm_disj_left JOIN t_wasm_disj_right ON t_wasm_disj_left.k = t_wasm_disj_right.k
    WHERE (t_wasm_disj_left.a = 1 AND t_wasm_disj_right.x = 100 AND wasm_disj_det(t_wasm_disj_left.k) % 2 = 0)
       OR (t_wasm_disj_left.a = 2 AND t_wasm_disj_right.x = 200)
) WHERE explain ILIKE '%Filter column:%' ORDER BY 1 FORMAT TSV;

-- Both give the same result, and the same as with the optimization disabled.
SELECT 'results';
SELECT count() FROM t_wasm_disj_left JOIN t_wasm_disj_right ON t_wasm_disj_left.k = t_wasm_disj_right.k
WHERE (t_wasm_disj_left.a = 1 AND t_wasm_disj_right.x = 100 AND wasm_disj_nondet(t_wasm_disj_left.k) % 2 = 0)
   OR (t_wasm_disj_left.a = 2 AND t_wasm_disj_right.x = 200);
SELECT count() FROM t_wasm_disj_left JOIN t_wasm_disj_right ON t_wasm_disj_left.k = t_wasm_disj_right.k
WHERE (t_wasm_disj_left.a = 1 AND t_wasm_disj_right.x = 100 AND wasm_disj_det(t_wasm_disj_left.k) % 2 = 0)
   OR (t_wasm_disj_left.a = 2 AND t_wasm_disj_right.x = 200);
SELECT count() FROM t_wasm_disj_left JOIN t_wasm_disj_right ON t_wasm_disj_left.k = t_wasm_disj_right.k
WHERE (t_wasm_disj_left.a = 1 AND t_wasm_disj_right.x = 100 AND wasm_disj_nondet(t_wasm_disj_left.k) % 2 = 0)
   OR (t_wasm_disj_left.a = 2 AND t_wasm_disj_right.x = 200)
SETTINGS use_join_disjunctions_push_down = 0;

DROP TABLE t_wasm_disj_left;
DROP TABLE t_wasm_disj_right;
DROP FUNCTION IF EXISTS wasm_disj_det;
DROP FUNCTION IF EXISTS wasm_disj_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_disj_test';
SQL
