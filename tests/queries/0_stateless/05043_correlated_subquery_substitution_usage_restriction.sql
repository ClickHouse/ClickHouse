-- Usage restriction for correlated-subquery equivalent-expression substitution:
-- a correlated column is substituted only when its sole uses inside the subquery
-- (per UNION-arm scope) are the recorded equality conjuncts. Any other expression over it
-- would be evaluated on the substituted inner values, including rows that never match any
-- outer value, so e.g. a division that is safe over the outer domain could throw; such
-- identifiers keep the CROSS JOIN fallback.
-- Parts 1/2 of the type-conversion coverage: 05023_correlated_subquery_equivalence_type_conversion
-- and 05042_correlated_subquery_equivalence_type_conversion_2 (this file continues their case
-- numbering at 33).
--
-- Each case prints a header line, then a `plan:` line probing EXPLAIN PLAN actions = 1:
--   substituted    - the substitution step is in the plan ("Renaming correlated columns ...")
--   fallback_arms  - for the UNION cases the substitution step description does not survive
--                    step merging inside union arms, so those probes count `ReadFromCommonBuffer`
--                    lines instead (= number of fallback arms reading the outer domain from
--                    the buffer)
-- and then the query results.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET correlated_subqueries_substitute_equivalent_expressions = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET correlated_subqueries_default_join_kind = 'left';
SET enable_parallel_replicas = 0;
-- Pinned because filter-to-Prewhere relocation, filter merging, and the join-order conversion
-- change which step descriptions are visible to the plan probes below.
SET query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_optimize_join_order_limit = 10;
-- Pinned off like in parts 1/2 (see issue #116358): the conversion changes which steps are
-- visible to the plan probes.
SET query_plan_merge_filter_into_join_condition = 0;

SELECT '-- Case 33: guarded, exact-type sibling member reachable through a cross-type bridge; the co-located intDiv is safe over the outer domain (2 - 1 = 1) but would see the inner value 2 after substitution and throw';
CREATE TABLE t_outer_33 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_33 (n Nullable(Int32), c Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_33 VALUES (1);
INSERT INTO t_inner_33 VALUES (1, 2), (2, 2);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_33 AS o WHERE EXISTS (SELECT 1 FROM t_inner_33 AS i WHERE i.n = o.x AND i.n = i.c AND intDiv(1, 2 - o.x) >= 0));
SELECT x FROM t_outer_33 AS o WHERE EXISTS (SELECT 1 FROM t_inner_33 AS i WHERE i.n = o.x AND i.n = i.c AND intDiv(1, 2 - o.x) >= 0) ORDER BY x;

SELECT '-- Case 34: guarded, the same channel with an exact-type member and no cross-type edge at all';
CREATE TABLE t_inner_34 (c Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_inner_34 VALUES (2);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_33 AS o WHERE EXISTS (SELECT 1 FROM t_inner_34 AS i WHERE i.c = o.x AND intDiv(1, 2 - o.x) >= 0));
SELECT x FROM t_outer_33 AS o WHERE EXISTS (SELECT 1 FROM t_inner_34 AS i WHERE i.c = o.x AND intDiv(1, 2 - o.x) >= 0) ORDER BY x;

SELECT '-- Case 36: guarded, the correlated column is used in the subquery projection';
CREATE TABLE t_outer_36 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_36 (x Nullable(Int32), y Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_36 VALUES (1), (3);
INSERT INTO t_inner_36 VALUES (1, 10), (NULL, 20);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_36 AS o WHERE EXISTS (SELECT i.y + o.x FROM t_inner_36 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_36 AS o WHERE EXISTS (SELECT i.y + o.x FROM t_inner_36 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 37: guarded, the correlated column is an aggregate argument / a user group key';
SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT sum(o.x) FROM t_inner_36 AS i WHERE i.x = o.x) FROM t_outer_36 AS o);
SELECT x, (SELECT sum(o.x) FROM t_inner_36 AS i WHERE i.x = o.x) FROM t_outer_36 AS o ORDER BY x;

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_36 AS o WHERE EXISTS (SELECT count() FROM t_inner_36 AS i WHERE i.x = o.x GROUP BY o.x));
SELECT x FROM t_outer_36 AS o WHERE EXISTS (SELECT count() FROM t_inner_36 AS i WHERE i.x = o.x GROUP BY o.x) ORDER BY x;

-- The UNION cases pin the default join kind: under the non-default 'left' kind the CROSS JOIN
-- fallback inside a correlated UNION is broken independently of this feature (a fallback arm
-- loses rows, and two fallback arms cannot build a pipeline).
SET correlated_subqueries_default_join_kind = 'right';

SELECT '-- Case 35: mixed UNION ALL arms, both orders; only the arm that uses the correlated column beyond the equality keeps the fallback (one ReadFromCommonBuffer), the equality-only arm is substituted; the use-arm carries a throwing canary (inner value 2 makes intDiv(1, 2 - o.x) throw if that arm were wrongly substituted)';
CREATE TABLE t_outer_35 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_35a (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_35b (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_35 VALUES (1), (3);
INSERT INTO t_inner_35a VALUES (1), (NULL);
INSERT INTO t_inner_35b VALUES (2), (3);

SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_35 AS o WHERE EXISTS (SELECT 1 FROM t_inner_35a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_35b AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1));
SELECT x FROM t_outer_35 AS o WHERE EXISTS (SELECT 1 FROM t_inner_35a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_35b AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1) ORDER BY x;

SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_35 AS o WHERE EXISTS (SELECT 1 FROM t_inner_35b AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1 UNION ALL SELECT 1 FROM t_inner_35a AS i WHERE i.x = o.x));
SELECT x FROM t_outer_35 AS o WHERE EXISTS (SELECT 1 FROM t_inner_35b AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1 UNION ALL SELECT 1 FROM t_inner_35a AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 38: shared ancestor above the union; both arms are guarded by the inherited usage set (two ReadFromCommonBuffer); before the restriction the substituted arms fed the inner value 2 to the ancestor intDiv and threw';
CREATE TABLE t_outer_38 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_38a (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_38b (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_38 VALUES (1);
INSERT INTO t_inner_38a VALUES (1), (2);
INSERT INTO t_inner_38b VALUES (2);

SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_38 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_38a AS i WHERE i.x = o.x UNION ALL SELECT x FROM t_inner_38b AS i WHERE i.x = o.x) AS u WHERE intDiv(1, 2 - o.x) >= 0));
SELECT x FROM t_outer_38 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_38a AS i WHERE i.x = o.x UNION ALL SELECT x FROM t_inner_38b AS i WHERE i.x = o.x) AS u WHERE intDiv(1, 2 - o.x) >= 0) ORDER BY x;

SELECT '-- Case 39: an n-ary UNION must decorrelate every arm — the handler previously rebuilt the union from only the first and last child, silently dropping middle arms';
CREATE TABLE t_outer_39 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_39a (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_39b (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_39c (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_39 VALUES (1), (2), (3);
INSERT INTO t_inner_39a VALUES (1);
INSERT INTO t_inner_39b VALUES (2);
INSERT INTO t_inner_39c VALUES (3);

SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_39 AS o WHERE EXISTS (SELECT 1 FROM t_inner_39a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39b AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39c AS i WHERE i.x = o.x));
SELECT x FROM t_outer_39 AS o WHERE EXISTS (SELECT 1 FROM t_inner_39a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39b AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39c AS i WHERE i.x = o.x) ORDER BY x;

-- Only the middle arm can match — this returned no rows before the fix.
SELECT x FROM t_outer_39 AS o WHERE o.x = 2 AND EXISTS (SELECT 1 FROM t_inner_39a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39b AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39c AS i WHERE i.x = o.x) ORDER BY x;

-- The middle arm uses the correlated column beyond the equality, so only it falls back; its
-- intDiv canary — inner value 2 — would throw if that arm were wrongly substituted; the outer
-- domain excludes 2 so the retained fallback conjunct never divides by zero.
CREATE TABLE t_outer_39m (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_39d (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_39m VALUES (1), (3);
INSERT INTO t_inner_39d VALUES (2), (3);

SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_39m AS o WHERE EXISTS (SELECT 1 FROM t_inner_39a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39d AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1 UNION ALL SELECT 1 FROM t_inner_39c AS i WHERE i.x = o.x));
SELECT x FROM t_outer_39m AS o WHERE EXISTS (SELECT 1 FROM t_inner_39a AS i WHERE i.x = o.x UNION ALL SELECT 1 FROM t_inner_39d AS i WHERE i.x = o.x AND intDiv(1, 2 - o.x) <= 1 UNION ALL SELECT 1 FROM t_inner_39c AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 40: an equality on a union output column recorded above the UNION (u.x = o.x) is translated into each arm (positionally, through pure renames only), so the arms can substitute the correlated column';
CREATE TABLE t_outer_40 (x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_40a (x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_40b (x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_40c (x Nullable(Int32), y Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_40 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t_inner_40a VALUES (1, 10), (5, 20);
INSERT INTO t_inner_40b VALUES (2, 20), (9, 999);
INSERT INTO t_inner_40c VALUES (NULL, 30), (3, 30);

-- Both arms substitute — the parent equality reaches them through the rename bridge and the positional mapping.
SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40b AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x));
SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40b AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x) ORDER BY x;

-- A computed union column is never traced (pure renames only) — both arms keep the fallback and the results stay correct.
SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT abs(x) AS x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT abs(x) AS x FROM t_inner_40b AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x));
SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT abs(x) AS x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT abs(x) AS x FROM t_inner_40b AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x) ORDER BY x;

-- Union type coercion — the arm whose column goes through the coercion function keeps the fallback (the trace follows pure renames only), the same-type arm substitutes; the inner NULL never matches.
SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40c AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x));
SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40c AS t2 WHERE t2.y = o.y) AS u WHERE u.x = o.x) ORDER BY x;

-- The second arm uses o.y beyond its equality, so only it keeps the fallback; its intDiv canary (inner row (9, 999)
-- matching no outer row) would throw only if that arm were wrongly substituted — the retained fallback conjunct evaluates on the outer domain, which never hits the zero denominator.
SELECT format('plan: fallback_arms={}',
              toString(countIf(explain LIKE '%ReadFromCommonBuffer%')))
FROM (EXPLAIN PLAN actions = 1 SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40b AS t2 WHERE t2.y = o.y AND intDiv(1, 999 - o.y) <= 1) AS u WHERE u.x = o.x));
SELECT x, y FROM t_outer_40 AS o WHERE EXISTS (SELECT 1 FROM (SELECT x FROM t_inner_40a AS t1 WHERE t1.y = o.y UNION ALL SELECT x FROM t_inner_40b AS t2 WHERE t2.y = o.y AND intDiv(1, 999 - o.y) <= 1) AS u WHERE u.x = o.x) ORDER BY x;
