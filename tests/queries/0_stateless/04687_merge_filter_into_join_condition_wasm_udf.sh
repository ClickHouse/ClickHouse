#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the fast build has no WebAssembly engine.
# no-msan: WebAssembly UDFs are not run under MSan, like every other wasm test.
# Module and function names are derived from CLICKHOUSE_DATABASE, so parallel copies of this test
# never collide and no-parallel is not needed.

# A WebAssembly UDF declared without DETERMINISTIC redraws per evaluation, exactly like `rand`, so it
# must not be promoted to a JOIN key while the conjunct above the join is replaced by a constant. Its
# name is chosen by the user, so unlike the builtins of the same class it cannot be refused by a name
# list; the refusal has to come from the function reporting its own contract. The DETERMINISTIC twin is
# the control: it must stay eligible, so the refusal keys on the declaration and not on the call being
# a UDF at all. Neither is constant-folded (`isSuitableForConstantFolding` is also the DETERMINISTIC
# flag), so both reach the pass as ordinary function nodes and neither row is vacuous.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE="mod_${CLICKHOUSE_DATABASE}"
FN_ND="wasm_nd_${CLICKHOUSE_DATABASE}"
FN_D="wasm_d_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_ND}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP FUNCTION IF EXISTS ${FN_D}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'" 2>/dev/null
}
# CREATE FUNCTION is server-global, so drop both UDFs on every exit path.
trap cleanup EXIT

cleanup

${CLICKHOUSE_CLIENT} -q "INSERT INTO system.webassembly_modules (name, code)
    SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}"/wasm/host_api.wasm

${CLICKHOUSE_CLIENT} <<EOF
SET enable_analyzer = 1;                          -- the pass only sees JoinStepLogical
SET enable_parallel_replicas = 0;                 -- ditto
SET query_plan_join_swap_table = 0;               -- a swap changes which side is which
SET query_plan_optimize_join_order_randomize = 0; -- the plan-shape rows assert on join order
SET enable_join_runtime_filters = 0;              -- a runtime filter adds terms to the plan text
SET explain_query_plan_default = 'legacy';        -- \`Clauses:\` is only printed by the legacy format

-- \`test_random\` fills its result from thread_local_rng on every call, which is what makes the
-- promoted key draw a value independent of the conjunct this pass overwrites.
CREATE OR REPLACE FUNCTION ${FN_ND} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random'
    ARGUMENTS (UInt32) RETURNS UInt32;
CREATE OR REPLACE FUNCTION ${FN_D} LANGUAGE WASM ABI ROW_DIRECT FROM '${MODULE}' :: 'test_random'
    ARGUMENTS (UInt32) RETURNS UInt32 DETERMINISTIC;

DROP TABLE IF EXISTS lw SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS rw SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE lw (k UInt32, a UInt32) ENGINE = Log;
CREATE TABLE rw (k UInt32, b UInt8) ENGINE = Log;
INSERT INTO lw SELECT number % 16, number FROM numbers(20000);
INSERT INTO rw SELECT number % 16, number % 16 FROM numbers(320);

-- Fixture premise: the UDF really does redraw, else every row below is vacuous. The argument must be
-- \`materialize\`d and the calls must be separate ROWS: two textually identical calls on a literal are
-- one shared node after common-subexpression elimination, so they return the same draw and the
-- premise would read 0 on a redrawing function.
SELECT '-- the non-DETERMINISTIC UDF redraws per evaluation';
SELECT uniqExact(${FN_ND}(materialize(1 :: UInt32))) > 1 FROM numbers(50);

-- L15: the promoted key would draw its own value, so the surviving rows do not satisfy the predicate
-- the user wrote. Asserted on the plan rather than on the result: reproducing the wrong VALUE needs
-- the distributed vehicle the server-constant rows use, whereas the plan oracle pins exactly the
-- claim, that the conjunct was not merged into the join.
SELECT '-- a non-DETERMINISTIC WebAssembly UDF is not promoted to a JOIN key';
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 0 FROM (
    EXPLAIN PLAN actions = 1 SELECT rw.b FROM lw INNER JOIN rw ON lw.k = rw.k
    WHERE toUInt8(${FN_ND}(toUInt32(lw.a)) % 16) = rw.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

-- C10: the same module and the same query shape, declared DETERMINISTIC, must still be merged.
-- Without this row L15 could be passing merely because the function is a UDF.
SELECT '-- a DETERMINISTIC WebAssembly UDF is still promoted';
SELECT countIf(explain ILIKE '%Clauses:%' AND explain ILIKE '%__table1.k, %') = 1 FROM (
    EXPLAIN PLAN actions = 1 SELECT rw.b FROM lw INNER JOIN rw ON lw.k = rw.k
    WHERE toUInt8(${FN_D}(toUInt32(lw.a)) % 16) = rw.b
    SETTINGS query_plan_merge_filter_into_join_condition = 1);

DROP TABLE lw SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE rw SETTINGS ignore_drop_queries_probability = 0;
EOF
