-- Correlated-subquery equivalent-expression substitution across compatible types:
-- nullability-only differences and native-number pairs with a least supertype.
-- Cross-base pairs require an integer correlated base: float equality does not pin the
-- representation, so float-correlated pairs are guarded (case 12).
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/113407
-- Part 2: 05042_correlated_subquery_equivalence_type_conversion_2 continues with LowCardinality, Bool, signed-zero, and DateTime cases.
--
-- Each case prints a header line, then a `plan:` line probing EXPLAIN PLAN actions = 1:
--   cross_joins      - number of plan lines mentioning a cross join (0 = the fallback was avoided)
--   substituted      - the substitution step is in the plan ("Renaming correlated columns ...")
--   null_prefiltered - the NULL/unconvertible-value pre-filter step is in the plan
--                      ("Filter values of expressions equivalent ..."); expected exactly for the
--                      assumeNotNull / accurateCastOrNull conversions, absent for lossless ones
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
-- Pinned off to isolate the guarded float case 12 (and case 24 in part 2) from issue #116358: under the default,
-- merging the fallback's cross-type equality into a join condition compares floats bitwise and
-- loses the `-0.0` matches that `equals` semantics keep.
SET query_plan_merge_filter_into_join_condition = 0;

SELECT '-- Case 1: the reproducer from the issue; the nullability mismatch must not force a CROSS JOIN';
-- The NULL pre-filter is pushed past the subquery join into the `store_sales` Prewhere,
-- so its step description is not asserted here (the two-table cases below pin it).
CREATE TABLE customer (c_customer_sk Int64 NOT NULL, c_current_cdemo_sk Nullable(Int64), c_current_addr_sk Nullable(Int64)) ENGINE=MergeTree ORDER BY c_customer_sk;
CREATE TABLE customer_address (ca_address_sk Int64 NOT NULL) ENGINE=MergeTree ORDER BY ca_address_sk;
CREATE TABLE customer_demographics (cd_demo_sk Int64 NOT NULL) ENGINE=MergeTree ORDER BY cd_demo_sk;
CREATE TABLE store_sales (ss_customer_sk Nullable(Int64), ss_sold_date_sk Nullable(UInt32)) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE date_dim (d_date_sk UInt32 NOT NULL, d_year UInt32, d_qoy UInt32) ENGINE=MergeTree ORDER BY d_date_sk;
INSERT INTO customer VALUES (1,10,100);
INSERT INTO customer_address VALUES (100);
INSERT INTO customer_demographics VALUES (10);
INSERT INTO date_dim VALUES (1,2002,1);
INSERT INTO store_sales VALUES (1,1);

SELECT format('plan: cross_joins={} substituted={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT * FROM customer c, customer_address ca, customer_demographics cd
    WHERE c.c_current_addr_sk = ca.ca_address_sk AND cd.cd_demo_sk = c.c_current_cdemo_sk
    AND EXISTS (
        SELECT * FROM store_sales, date_dim
        WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
          AND d_year = 2002 AND d_qoy < 4
    )
);
SELECT c_customer_sk, c_current_cdemo_sk, c_current_addr_sk, ca_address_sk, cd_demo_sk
FROM customer c, customer_address ca, customer_demographics cd
WHERE c.c_current_addr_sk = ca.ca_address_sk AND cd.cd_demo_sk = c.c_current_cdemo_sk
AND EXISTS (
    SELECT * FROM store_sales, date_dim
    WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
      AND d_year = 2002 AND d_qoy < 4
)
ORDER BY c_customer_sk;

SELECT '-- Case 2: EXISTS, outer Int64 vs inner Nullable(Int64); inner NULL must not match, in particular not outer 0';
CREATE TABLE t_outer_2 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_2 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_2 VALUES (0), (1), (2), (3);
INSERT INTO t_inner_2 VALUES (NULL), (1), (NULL), (3), (42);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_2 AS o WHERE EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_2 AS o WHERE EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 3: NOT EXISTS complement of case 2';
SELECT x FROM t_outer_2 AS o WHERE NOT EXISTS (SELECT 1 FROM t_inner_2 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 4: EXISTS, outer Nullable(Int64) vs inner Int64; outer NULL rows must not match';
CREATE TABLE t_outer_4 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_4 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_4 VALUES (NULL), (0), (1), (2);
INSERT INTO t_inner_4 VALUES (1), (5);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_4 AS o WHERE EXISTS (SELECT 1 FROM t_inner_4 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_4 AS o WHERE EXISTS (SELECT 1 FROM t_inner_4 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 5: scalar subquery with count(); outer 0 must count only genuine 0 rows, not inner NULLs';
CREATE TABLE t_outer_5 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_5 (x Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_5 VALUES (0), (1), (2);
INSERT INTO t_inner_5 VALUES (NULL), (NULL), (0), (1), (1);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_5 AS i WHERE i.x = o.x) FROM t_outer_5 AS o);
SELECT x, (SELECT count() FROM t_inner_5 AS i WHERE i.x = o.x) FROM t_outer_5 AS o ORDER BY x;

SELECT '-- Case 6: guarded, the correlated column is also used in a second expression inside the subquery; after substitution that expression would be evaluated on the inner values, so the identifier is not substituted';
CREATE TABLE t_outer_6 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_6 (x Nullable(Int64), y Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_6 VALUES (0), (1), (2), (3);
INSERT INTO t_inner_6 VALUES (NULL, 100), (1, 10), (2, 1), (3, 3);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_6 AS o WHERE EXISTS (SELECT 1 FROM t_inner_6 AS i WHERE i.x = o.x AND i.y + o.x > 5));
SELECT x FROM t_outer_6 AS o WHERE EXISTS (SELECT 1 FROM t_inner_6 AS i WHERE i.x = o.x AND i.y + o.x > 5) ORDER BY x;

SELECT '-- Case 7: widening, outer Int64 vs inner Int32 (lossless cast, no pre-filter); 2147483648 is out of the inner range';
CREATE TABLE t_outer_7 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_7 (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_7 VALUES (-5), (3), (7), (2147483648);
INSERT INTO t_inner_7 VALUES (-5), (7), (100);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_7 AS o WHERE EXISTS (SELECT 1 FROM t_inner_7 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_7 AS o WHERE EXISTS (SELECT 1 FROM t_inner_7 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 8: outer Int64 vs inner Nullable(Int32) (nullability and width differ)';
CREATE TABLE t_inner_8 (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_inner_8 VALUES (NULL), (3), (100);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_7 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_7 AS o WHERE EXISTS (SELECT 1 FROM t_inner_8 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 9: narrowing, outer Int32 vs inner Int64; 5000000000 must not match 705032704 (= 5000000000 mod 2^32)';
CREATE TABLE t_outer_9 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_9 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_9 VALUES (1), (2), (705032704);
INSERT INTO t_inner_9 VALUES (1), (5000000000), (-9000000000);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_9 AS o WHERE EXISTS (SELECT 1 FROM t_inner_9 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_9 AS o WHERE EXISTS (SELECT 1 FROM t_inner_9 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 10: sideways, outer Int32 vs inner UInt32 (supertype Int64); 3000000000 must not match -1294967296 (its Int32 wrap)';
CREATE TABLE t_outer_10 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_10 (x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_10 VALUES (-1294967296), (5), (42);
INSERT INTO t_inner_10 VALUES (3000000000), (5);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_10 AS o WHERE EXISTS (SELECT 1 FROM t_inner_10 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_10 AS o WHERE EXISTS (SELECT 1 FROM t_inner_10 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 11: outer Int32 vs inner Float64; 1.5 must not match 1 or 2 (no truncation), 3.0 must match 3, and inner -0.0 canonicalizes to 0 through accurateCastOrNull and must match outer 0';
CREATE TABLE t_outer_11 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_11 (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_11 VALUES (0), (1), (2), (3);
INSERT INTO t_inner_11 VALUES (1.5), (3.0), (-0.0);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_11 AS o WHERE EXISTS (SELECT 1 FROM t_inner_11 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_11 AS o WHERE EXISTS (SELECT 1 FROM t_inner_11 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 12: guarded, outer Float64 vs inner Int32; a float correlated value cannot be reconstructed from an equivalent member (-0.0 = 0 is true but hash joins compare floats bitwise), so with the equality kept as a filter (see issue #116358) the fallback matches -0.0 to inner 0 (the -0 row appears) while 1.5 and 100.25 must not match';
CREATE TABLE t_outer_12 (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_12 (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_12 VALUES (1.5), (3), (100.25), (-0.0);
INSERT INTO t_inner_12 VALUES (1), (3), (100), (0);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_12 AS o WHERE EXISTS (SELECT 1 FROM t_inner_12 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_12 AS o WHERE EXISTS (SELECT 1 FROM t_inner_12 AS i WHERE i.x = o.x) ORDER BY x;

-- Like case 12 above, the pairs below are guarded and must NOT be substituted; here the
-- comparison semantics are not consistent with CAST (the fallback plan shape itself is not pinned).

SELECT '-- Case 13: guarded, outer Float64 vs inner Int64; the comparison is exact, 9007199254740992.0 must not match 9007199254740993';
CREATE TABLE t_outer_13 (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_13 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_13 VALUES (9007199254740992), (123);
INSERT INTO t_inner_13 VALUES (9007199254740993), (123);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_13 AS o WHERE EXISTS (SELECT 1 FROM t_inner_13 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_13 AS o WHERE EXISTS (SELECT 1 FROM t_inner_13 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 14: guarded, outer Int64 vs inner UInt64; -1 must not match 18446744073709551615';
CREATE TABLE t_outer_14 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_14 (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_14 VALUES (-1), (42), (9223372036854775807);
INSERT INTO t_inner_14 VALUES (42), (9223372036854775808), (18446744073709551615);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_14 AS o WHERE EXISTS (SELECT 1 FROM t_inner_14 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_14 AS o WHERE EXISTS (SELECT 1 FROM t_inner_14 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 15: guarded, String vs FixedString(2); the comparison ignores padding, so ''a'' must match toFixedString(''a'', 2)';
CREATE TABLE t_outer_15 (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_15 (s FixedString(2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_15 VALUES ('a'), ('bb'), ('zz');
INSERT INTO t_inner_15 VALUES ('a'), ('bb');

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_outer_15 AS o WHERE EXISTS (SELECT 1 FROM t_inner_15 AS i WHERE i.s = o.s));
SELECT s FROM t_outer_15 AS o WHERE EXISTS (SELECT 1 FROM t_inner_15 AS i WHERE i.s = o.s) ORDER BY s;
