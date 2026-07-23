-- Tags: no-fasttest, no-msan
-- no-fasttest, no-msan: requires USE_EMBEDDED_COMPILER, which those builds disable; without
-- JIT the expression is never compiled and the test would pass vacuously.

-- Regression test for issue #111485: the JIT compilability gate must reject a function whose
-- child operand has a non-native result type (here `Nullable(Nothing)`), otherwise it raises
-- the `Invalid cast from Nothing to native type` exception (an abort in debug/sanitizer
-- builds). The query must simply complete. query_plan_merge_filters is pinned because the
-- offending and(Nullable(Nothing), ...) node only forms when the adjacent filters are merged.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_jit_nothing;
CREATE TABLE t_jit_nothing (uid Int16, age Int16) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_jit_nothing VALUES (1, 10), (2, 20);

SELECT * FROM
(
    SELECT * FROM t_jit_nothing AS u1 ANTI RIGHT JOIN t_jit_nothing AS u2 ON 1
    WHERE NULL[u1.uid]
    QUALIFY materialize(NULL)
)
WHERE age > 5
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, query_plan_merge_filters = 1;

DROP TABLE t_jit_nothing;

SELECT 'ok';
