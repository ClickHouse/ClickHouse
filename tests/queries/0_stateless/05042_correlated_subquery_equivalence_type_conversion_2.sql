-- Correlated-subquery equivalent-expression substitution across compatible types, part 2:
-- LowCardinality wrappers, scalar count() through conversions, Bool, signed zero,
-- and DateTime attribute cases.
-- Part 1: 05023_correlated_subquery_equivalence_type_conversion.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/113407
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
-- Pinned off to isolate the guarded float case 24 (and case 12 in part 1) from issue #116358: under the default,
-- merging the fallback's cross-type equality into a join condition compares floats bitwise and
-- loses the `-0.0` matches that `equals` semantics keep.
SET query_plan_merge_filter_into_join_condition = 0;

-- LowCardinality is a value-transparent wrapper: only the base types decide the conversion,
-- so LC pairs are substituted (cases 16-20) unless the base itself is excluded (case 21).

SELECT '-- Case 16: outer String vs inner LowCardinality(String); lossless rewrap, no pre-filter';
CREATE TABLE t_outer_16 (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_16 (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_16 VALUES ('a'), ('b'), ('zz');
INSERT INTO t_inner_16 VALUES ('a'), ('zz'), ('q');

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_outer_16 AS o WHERE EXISTS (SELECT 1 FROM t_inner_16 AS i WHERE i.s = o.s));
SELECT s FROM t_outer_16 AS o WHERE EXISTS (SELECT 1 FROM t_inner_16 AS i WHERE i.s = o.s) ORDER BY s;

SELECT '-- Case 17: outer LowCardinality(String) vs inner String; lossless rewrap in the other direction';
CREATE TABLE t_outer_17 (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_17 (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_17 VALUES ('a'), ('b'), ('zz');
INSERT INTO t_inner_17 VALUES ('a'), ('q'), ('zz');

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_outer_17 AS o WHERE EXISTS (SELECT 1 FROM t_inner_17 AS i WHERE i.s = o.s));
SELECT s FROM t_outer_17 AS o WHERE EXISTS (SELECT 1 FROM t_inner_17 AS i WHERE i.s = o.s) ORDER BY s;

SELECT '-- Case 18: outer String vs inner LowCardinality(Nullable(String)); inner NULL must not match, in particular not outer ''''';
CREATE TABLE t_outer_18 (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_18 (s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_18 VALUES (''), ('a'), ('b');
INSERT INTO t_inner_18 VALUES (NULL), ('a'), (NULL), ('b'), ('zz');

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_outer_18 AS o WHERE EXISTS (SELECT 1 FROM t_inner_18 AS i WHERE i.s = o.s));
SELECT s FROM t_outer_18 AS o WHERE EXISTS (SELECT 1 FROM t_inner_18 AS i WHERE i.s = o.s) ORDER BY s;

-- LowCardinality of numeric types is suspicious by default; enabled here for the test only.
SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- Case 19: outer LowCardinality(Nullable(Int64)) vs inner Int32; lossless widening, outer NULL rows must not match';
CREATE TABLE t_outer_19 (x LowCardinality(Nullable(Int64))) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_19 (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_19 VALUES (NULL), (1), (2), (3);
INSERT INTO t_inner_19 VALUES (1), (3), (100);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_19 AS o WHERE EXISTS (SELECT 1 FROM t_inner_19 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_19 AS o WHERE EXISTS (SELECT 1 FROM t_inner_19 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 20: outer Int64 vs inner LowCardinality(Nullable(Int32)); inner NULL must not match, in particular not outer 0';
CREATE TABLE t_outer_20 (x Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_20 (x LowCardinality(Nullable(Int32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_20 VALUES (0), (1), (2), (3), (2147483648);
INSERT INTO t_inner_20 VALUES (NULL), (1), (NULL), (3), (42);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_20 AS o WHERE EXISTS (SELECT 1 FROM t_inner_20 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_20 AS o WHERE EXISTS (SELECT 1 FROM t_inner_20 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 21: guarded, outer String vs inner LowCardinality(FixedString(2)); the comparison ignores padding, so ''a'' must match toFixedString(''a'', 2)';
CREATE TABLE t_outer_21 (s String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_21 (s LowCardinality(FixedString(2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_21 VALUES ('a'), ('bb'), ('zz');
INSERT INTO t_inner_21 VALUES ('a'), ('bb');

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT s FROM t_outer_21 AS o WHERE EXISTS (SELECT 1 FROM t_inner_21 AS i WHERE i.s = o.s));
SELECT s FROM t_outer_21 AS o WHERE EXISTS (SELECT 1 FROM t_inner_21 AS i WHERE i.s = o.s) ORDER BY s;

-- Scalar count() through the accurate-cast pre-filter: the outer default value (0) is
-- sensitive to non-convertible inner values folding into the nested default, so the counts
-- (not only the plan probes) must show that they are filtered out. Unmatched outer rows
-- print NULL, as established in case 5.

SELECT '-- Case 22: scalar count() through narrowing, outer Int32 vs inner Int64; outer 0 must count only the genuine inner 0 (non-convertible values must not fold into it), and 705032704 (= 5000000000 mod 2^32) must not count 5000000000';
CREATE TABLE t_outer_22 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_22 (x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_22 VALUES (0), (1), (2), (705032704);
INSERT INTO t_inner_22 VALUES (0), (1), (1), (5000000000), (-9000000000);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_22 AS i WHERE i.x = o.x) FROM t_outer_22 AS o);
SELECT x, (SELECT count() FROM t_inner_22 AS i WHERE i.x = o.x) FROM t_outer_22 AS o ORDER BY x;

SELECT '-- Case 23: scalar count() through the float-to-int accurate cast, outer Int32 vs inner Float64; outer 0 must count only the genuine 0.0 (0.5 and 1.5 must not fold into 0), and 3.0 must count for 3';
CREATE TABLE t_outer_23 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_23 (x Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_23 VALUES (0), (1), (3);
INSERT INTO t_inner_23 VALUES (0.0), (0.5), (1.5), (3.0);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_23 AS i WHERE i.x = o.x) FROM t_outer_23 AS o);
SELECT x, (SELECT count() FROM t_inner_23 AS i WHERE i.x = o.x) FROM t_outer_23 AS o ORDER BY x;

SELECT '-- Case 24: guarded, outer Float64 vs inner Nullable(Float64); a float correlated value cannot be reconstructed exactly when the types differ: equals merges -0.0 and +0.0 (each outer zero counts both inner zeros, count 2 with the equality kept as a filter (see issue #116358)) while a substituted plan would group bitwise (count 1); inner NULL never counts';
CREATE TABLE t_outer_24 (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_24 (x Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_24 VALUES (-0.), (0.), (7.);
INSERT INTO t_inner_24 VALUES (NULL), (-0.), (0.), (7.);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_24 AS i WHERE i.x = o.x) FROM t_outer_24 AS o);
-- Ordered by the string form: the relative order of -0.0 and 0.0 under a float ORDER BY is comparator-dependent.
SELECT x, (SELECT count() FROM t_inner_24 AS i WHERE i.x = o.x) FROM t_outer_24 AS o ORDER BY toString(x);

SELECT '-- Case 25: guarded, outer Bool vs inner Int32; accurateCastOrNull to Bool maps any non-zero value to true instead of checking exactness, so true must count only the genuine inner 1 rows (never the 2) and false counts only the 0';
CREATE TABLE t_outer_25 (x Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_25 (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_25 VALUES (true), (false);
INSERT INTO t_inner_25 VALUES (0), (1), (1), (2);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_25 AS i WHERE i.x = o.x) FROM t_outer_25 AS o);
SELECT x, (SELECT count() FROM t_inner_25 AS i WHERE i.x = o.x) FROM t_outer_25 AS o ORDER BY x;

SELECT '-- Case 26: outer Int32 vs inner Bool; the member-side direction is faithful, CAST from Bool to Int32 is lossless (true is 1, false is 0), so 0 and 1 match while 2 must not';
CREATE TABLE t_outer_26 (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_26 (x Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_26 VALUES (0), (1), (2);
INSERT INTO t_inner_26 VALUES (true), (false);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_26 AS o WHERE EXISTS (SELECT 1 FROM t_inner_26 AS i WHERE i.x = o.x));
SELECT x FROM t_outer_26 AS o WHERE EXISTS (SELECT 1 FROM t_inner_26 AS i WHERE i.x = o.x) ORDER BY x;

SELECT '-- Case 27: guarded, outer Float64 with an exact-type member reached through a numeric bridge (i.a = o.x AND i.a = i.b); the float correlated column must not be reconstructed even from the exact-type member i.b, because the fallback''s equals keeps -0.0 matching the inner 0 while a substituted bitwise join would drop it';
CREATE TABLE t_outer_27 (x Float64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_27 (a Int32, b Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_27 VALUES (-0.), (1.);
INSERT INTO t_inner_27 VALUES (0, 0.), (1, 1.);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_27 AS o WHERE EXISTS (SELECT 1 FROM t_inner_27 AS i WHERE i.a = o.x AND i.a = i.b));
-- Ordered by the string form: the relative order of -0.0 and 0.0 under a float ORDER BY is comparator-dependent.
SELECT x FROM t_outer_27 AS o WHERE EXISTS (SELECT 1 FROM t_inner_27 AS i WHERE i.a = o.x AND i.a = i.b) ORDER BY toString(x);

SELECT '-- Case 28: outer Bool vs inner Nullable(Bool); the wrapper-only difference is faithful for Bool (isNotNull + assumeNotNull, canonical 0/1 values), so it is substituted; inner NULL never counts';
CREATE TABLE t_outer_28 (x Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_28 (x Nullable(Bool)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_28 VALUES (true), (false);
INSERT INTO t_inner_28 VALUES (NULL), (true), (true);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x, (SELECT count() FROM t_inner_28 AS i WHERE i.x = o.x) FROM t_outer_28 AS o);
SELECT x, (SELECT count() FROM t_inner_28 AS i WHERE i.x = o.x) FROM t_outer_28 AS o ORDER BY x;

SELECT '-- Case 29: guarded, outer Bool vs inner UInt8; Bool is a custom-named UInt8 and must not be reconstructed from a plain UInt8 member: the smuggled non-canonical 2 would reach expressions that are safe over the Bool domain (here intDiv(1, 2 - o.x) would throw)';
CREATE TABLE t_outer_29 (x Bool) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_29 (u UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_29 VALUES (true), (false);
INSERT INTO t_inner_29 VALUES (0), (1), (2);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_29 AS o WHERE EXISTS (SELECT 1 FROM t_inner_29 AS i WHERE i.u = o.x AND intDiv(1, 2 - o.x) >= 0));
SELECT x FROM t_outer_29 AS o WHERE EXISTS (SELECT 1 FROM t_inner_29 AS i WHERE i.u = o.x AND intDiv(1, 2 - o.x) >= 0) ORDER BY x;

SELECT '-- Case 30: guarded, outer DateTime(''UTC'') vs inner DateTime(''Asia/Tokyo''); ->equals ignores the DateTime timezone, but the timezone changes meaning: a substituted Asia/Tokyo member would make toHour(o.ts) see hour 9 instead of UTC hour 0 and drop the row';
CREATE TABLE t_outer_30 (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_30 (ts DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_30 VALUES (0);
INSERT INTO t_inner_30 VALUES (0);

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT ts FROM t_outer_30 AS o WHERE EXISTS (SELECT 1 FROM t_inner_30 AS i WHERE i.ts = o.ts AND toHour(o.ts) = 0));
SELECT ts FROM t_outer_30 AS o WHERE EXISTS (SELECT 1 FROM t_inner_30 AS i WHERE i.ts = o.ts AND toHour(o.ts) = 0) ORDER BY ts;

SELECT '-- Case 31: outer DateTime(''UTC'') vs inner Nullable(DateTime(''UTC'')); attribute-identical non-numeric types remain substitutable: the name-based check admits DateTime(''UTC'') against Nullable(DateTime(''UTC'')); inner NULL never matches';
CREATE TABLE t_outer_31 (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_31 (ts Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_31 VALUES (0), (3600);
INSERT INTO t_inner_31 VALUES (NULL), (0);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT ts FROM t_outer_31 AS o WHERE EXISTS (SELECT 1 FROM t_inner_31 AS i WHERE i.ts = o.ts));
SELECT ts FROM t_outer_31 AS o WHERE EXISTS (SELECT 1 FROM t_inner_31 AS i WHERE i.ts = o.ts) ORDER BY ts;

SELECT '-- Case 32: outer UInt8 vs inner Bool; the member''s Bool custom name must be normalized away by a CAST (not an alias) so that the correlated column keeps UInt8 rendering; a query that also uses toString(o.x) inside the subquery is guarded by the usage restriction';
CREATE TABLE t_outer_32 (x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_32 (b Bool) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_outer_32 VALUES (0), (1), (2);
INSERT INTO t_inner_32 VALUES (true), (false);

SELECT format('plan: cross_joins={} substituted={} null_prefiltered={}',
              toString(countIf(explain ILIKE '%cross%')),
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0),
              toString(countIf(explain LIKE '%Filter values of expressions equivalent to correlated columns that cannot match%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_32 AS o WHERE EXISTS (SELECT 1 FROM t_inner_32 AS i WHERE i.b = o.x));
SELECT x FROM t_outer_32 AS o WHERE EXISTS (SELECT 1 FROM t_inner_32 AS i WHERE i.b = o.x) ORDER BY x;

SELECT format('plan: substituted={}',
              toString(countIf(explain LIKE '%Renaming correlated columns to equivalent expressions in subquery%') > 0))
FROM (EXPLAIN PLAN actions = 1 SELECT x FROM t_outer_32 AS o WHERE EXISTS (SELECT 1 FROM t_inner_32 AS i WHERE i.b = o.x AND toString(o.x) = '1'));
SELECT x FROM t_outer_32 AS o WHERE EXISTS (SELECT 1 FROM t_inner_32 AS i WHERE i.b = o.x AND toString(o.x) = '1') ORDER BY x;
