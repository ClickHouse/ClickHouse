-- Regression test for https://github.com/ClickHouse/clickhouse-private/issues/56479
--
-- A query shaped as `FROM <t1> FINAL JOIN <t2> ON ... WHERE <t2>.<non_key> = ...` must push the
-- WHERE on the joined-to table `t2` into PREWHERE on `t2`, and must NOT read `t2` with FINAL.
--
-- Bug: `SelectQueryInfo::isFinal()` fell back to `ASTSelectQuery::final()` (the *first* table
-- expression's FINAL flag) when the per-table `table_expression_modifiers` were absent. So the read
-- of `t2` was treated as FINAL too, which (a) made `MergeTreeWhereOptimizer` keep only sorting-key
-- conditions in PREWHERE, dropping the `t2.v` filter from PREWHERE, and (b) forced a needless
-- in-order FINAL merge of `t2`.

DROP TABLE IF EXISTS t1_final_join;
DROP TABLE IF EXISTS t2_final_join;

CREATE TABLE t1_final_join (id Int32, s String) ENGINE = ReplacingMergeTree ORDER BY id;
CREATE TABLE t2_final_join (id Int32, v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1_final_join VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t2_final_join VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- `v` is not part of t2's sorting key, so it can only be moved to PREWHERE if the read of t2 is
-- correctly recognized as non-FINAL.

-- Pin prewhere-related settings so randomized test settings do not disable PREWHERE altogether.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

SET enable_analyzer = 1;
SELECT 'analyzer prewhere on joined-to table', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT t2_final_join.v, t1_final_join.s
    FROM t1_final_join FINAL
    JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
    WHERE t2_final_join.v = 'x'
)
WHERE explain LIKE '%Prewhere filter%';

SET enable_analyzer = 0;
SELECT 'legacy prewhere on joined-to table', count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT t2_final_join.v, t1_final_join.s
    FROM t1_final_join FINAL
    JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
    WHERE t2_final_join.v = 'x'
)
WHERE explain LIKE '%Prewhere filter%';

-- Results must stay correct in both modes.
SET enable_analyzer = 1;
SELECT 'result analyzer', t2_final_join.v, t1_final_join.s
FROM t1_final_join FINAL
JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
WHERE t2_final_join.v = 'x';

SET enable_analyzer = 0;
SELECT 'result legacy', t2_final_join.v, t1_final_join.s
FROM t1_final_join FINAL
JOIN t2_final_join ON t2_final_join.id = t1_final_join.id
WHERE t2_final_join.v = 'x';

DROP TABLE t1_final_join;
DROP TABLE t2_final_join;
