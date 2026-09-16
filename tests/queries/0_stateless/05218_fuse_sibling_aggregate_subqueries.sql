SET enable_analyzer = 1; -- FuseSiblingAggregateSubqueriesPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer
-- The fused branches must read one snapshot of each table, so the pass refuses when sibling branches
-- can capture their own. CI randomizes this setting, and arm 25 toggles it deliberately.
SET enable_shared_storage_snapshot_in_query = 1;

-- Each arm prints its answer with the optimization off and then on (the two lines must agree), plus a
-- query-tree assertion, because equal answers alone cannot tell a correct rewrite from no rewrite.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tn;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS lo;
DROP TABLE IF EXISTS dflt;
DROP TABLE IF EXISTS dec;
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS arr;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS rmt;
DROP TABLE IF EXISTS rp;

CREATE TABLE t (k Int64, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, number * 2 FROM numbers(1000);

CREATE TABLE tn (k Nullable(Int64), v Int64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tn SELECT number, number * 2, toString(number % 10) FROM numbers(1000);

-- k is not the sorting key, so no primary-key range pruning can rescue a throwing conjunct.
CREATE TABLE m (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO m SELECT number, number FROM numbers(10);

-- Holds the Int64 minimum, the value intDiv(k, -1) overflows on.
CREATE TABLE lo (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lo VALUES (1, -9223372036854775808), (2, 5), (3, 7);

-- x is added after the parts are written, so the parts do not store it and reading a row evaluates its
-- DEFAULT expression inside the reader.
CREATE TABLE dflt (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dflt VALUES (0), (1), (2);
ALTER TABLE dflt ADD COLUMN x Int64 DEFAULT k * 10;

CREATE TABLE dec (id Int64, p Decimal(7, 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO dec SELECT number, number FROM numbers(100);

CREATE TABLE f1 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f1 SELECT number % 10, number % 5 FROM numbers(200);
CREATE TABLE f2 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f2 SELECT number % 10, number % 5 FROM numbers(200);

CREATE TABLE arr (k Int64, a Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO arr VALUES (1, [1, 2]), (2, [3, 4]);

CREATE TABLE al (k Int64, y Int64 ALIAS k + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO al (k) SELECT number FROM numbers(100);

CREATE TABLE rmt (k Int64, v Int64) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO rmt SELECT number, number * 2 FROM numbers(1000);

CREATE TABLE rp (k Int64, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rp SELECT number, number * 2 FROM numbers(1000);

SELECT '-- 01 two sibling branches over one table: fuses';
SELECT '01', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '01', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '01 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 02 a Nullable(UInt8) and a LowCardinality(UInt8) residual: both fuse';
SELECT '02a', a, b FROM (SELECT count() AS a FROM tn WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '02a', a, b FROM (SELECT count() AS a FROM tn WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '02a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM tn WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '02b', a, b FROM (SELECT count() AS a FROM tn WHERE v > 10 AND lc = '3') AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND lc = '5') AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '02b', a, b FROM (SELECT count() AS a FROM tn WHERE v > 10 AND lc = '3') AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND lc = '5') AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '02b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM tn WHERE v > 10 AND lc = '3') AS x, (SELECT count() AS b FROM tn WHERE v > 10 AND lc = '5') AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

-- Only the refusal is asserted for 03, 03b and 03c: what protects the unfused form is plan-level filter
-- push-down rather than the AND itself, so executing it would measure master's plan choices, and it throws
-- under some randomized settings with the optimization off as well.
SELECT '-- 03 a conjunct that can throw for a row the branch excluded is refused';
SELECT '03 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k != 0 AND intDiv(100, k) > 40) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '03 fused disable', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k != 0 AND intDiv(100, k) > 40) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'disable');
SELECT '03 fused enable', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k != 0 AND intDiv(100, k) > 40) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'enable');
SELECT '03 fused force', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k != 0 AND intDiv(100, k) > 40) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'force_enable');

SELECT '-- 03b intDiv(k, -1) throws while canThrow answers false, so a blacklist would let it through';
SELECT '03b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM lo WHERE k > -9223372036854775808 AND intDiv(k, -1) < 0) AS x, (SELECT count() AS b FROM lo WHERE k = 5) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 03c the guard also covers a shared conjunct, whose position in the fused AND changes';
SELECT '03c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k != 0 AND intDiv(1, k) > 0) AS x, (SELECT count() AS b FROM m WHERE k > 0 AND intDiv(1, k) > 0) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 04 a branch whose conjuncts are all shared has no -If and keeps every row the shared part keeps';
SELECT '04', a, b FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '04', a, b FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '04 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 05 a filter that is not reproducible keeps its own scan';
SELECT '05 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND rand() < 100) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '06a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND rowNumberInAllBlocks() < 5) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '06b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k > 8 AND sleepEachRow(0.001) = 0) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 07 now() is deterministic within one query, so it does not block fusion';
SELECT '07', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '07', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '07 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 08 a NULL -If condition excludes its row exactly as a NULL WHERE does';
SELECT '08', a, b FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '08', a, b FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '08 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 09 an empty branch, and the same under aggregate_functions_null_for_empty';
SELECT '09a', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '09a', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '09b', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, aggregate_functions_null_for_empty = 1;
SELECT '09b', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, aggregate_functions_null_for_empty = 1;

SELECT '-- 10 with empty_result_for_aggregation_by_empty_set an empty branch empties the whole join, which one fused aggregation cannot reproduce';
SELECT '10', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, empty_result_for_aggregation_by_empty_set = 1;
SELECT '10', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, empty_result_for_aggregation_by_empty_set = 1;
SELECT '10 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, empty_result_for_aggregation_by_empty_set = 1);

SELECT '-- 11 an aggregate with an argument is out of scope: the -If condition would stop protecting it';
SELECT '11a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(DISTINCT k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(DISTINCT k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT avg(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT avg(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11c', a, b FROM (SELECT avg(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT avg(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '11c', a, b FROM (SELECT avg(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT avg(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
-- count(1) reaches this pass argument-less only while count-variant normalization is on, which is
-- what puts it in scope; the setting is pinned because CI randomizes it.
SELECT '11d', a, b FROM (SELECT count(1) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(1) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, optimize_normalize_count_variants = 1;
SELECT '11d', a, b FROM (SELECT count(1) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(1) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_normalize_count_variants = 1;
SELECT '11d fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(1) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(1) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_normalize_count_variants = 1);

SELECT '-- 12 a residual that is not a UInt8 condition cannot become an -If argument';
SELECT '12', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '12', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '12 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 13 two branches whose outputs share a name cannot both live in one projection';
SELECT '13', x.a, y.a FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS a FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '13', x.a, y.a FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS a FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '13 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT x.a, y.a FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS a FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 14 a non-fusable sibling in the same join does not stop the others';
SELECT '14', a, b, c FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y, (SELECT count() AS c FROM t GROUP BY k % 2 ORDER BY c LIMIT 1) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '14', a, b, c FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y, (SELECT count() AS c FROM t GROUP BY k % 2 ORDER BY c LIMIT 1) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '14 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y, (SELECT count() AS c FROM t GROUP BY k % 2 ORDER BY c LIMIT 1) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 15 a table-expression modifier is part of the source, so branches differing in FINAL are not fused; a branch reading FINAL is refused outright';
SELECT '15a', a, b FROM (SELECT count() AS a FROM rmt FINAL WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rmt WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '15a', a, b FROM (SELECT count() AS a FROM rmt FINAL WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rmt WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '15a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM rmt FINAL WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rmt WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '15b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM rmt FINAL WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rmt FINAL WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '15c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM rmt WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rmt WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 16 an ALIAS column carries an expression whose own column sources are not rebuilt';
SELECT '16', a, b FROM (SELECT count() AS a FROM al WHERE y = 30) AS x, (SELECT count() AS b FROM al WHERE y = 50) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '16', a, b FROM (SELECT count() AS a FROM al WHERE y = 30) AS x, (SELECT count() AS b FROM al WHERE y = 50) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '16 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM al WHERE y = 30) AS x, (SELECT count() AS b FROM al WHERE y = 50) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 17 the enclosing query keeps referring to both branches after one of them is fused away';
SELECT '17', a / b AS r FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y WHERE a > 0 ORDER BY r SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '17', a / b AS r FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y WHERE a > 0 ORDER BY r SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '17 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT a / b AS r FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y WHERE a > 0 ORDER BY r SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 18 eight branches, the arity TPC-DS query 88 uses';
SELECT '18', h1, h2, h3, h4, h5, h6, h7, h8 FROM (SELECT count() AS h1 FROM t WHERE v > 10 AND k = 100) AS s1, (SELECT count() AS h2 FROM t WHERE v > 10 AND k = 200) AS s2, (SELECT count() AS h3 FROM t WHERE v > 10 AND k = 300) AS s3, (SELECT count() AS h4 FROM t WHERE v > 10 AND k = 400) AS s4, (SELECT count() AS h5 FROM t WHERE v > 10 AND k = 500) AS s5, (SELECT count() AS h6 FROM t WHERE v > 10 AND k = 600) AS s6, (SELECT count() AS h7 FROM t WHERE v > 10 AND k = 700) AS s7, (SELECT count() AS h8 FROM t WHERE v > 10 AND k = 800) AS s8 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '18', h1, h2, h3, h4, h5, h6, h7, h8 FROM (SELECT count() AS h1 FROM t WHERE v > 10 AND k = 100) AS s1, (SELECT count() AS h2 FROM t WHERE v > 10 AND k = 200) AS s2, (SELECT count() AS h3 FROM t WHERE v > 10 AND k = 300) AS s3, (SELECT count() AS h4 FROM t WHERE v > 10 AND k = 400) AS s4, (SELECT count() AS h5 FROM t WHERE v > 10 AND k = 500) AS s5, (SELECT count() AS h6 FROM t WHERE v > 10 AND k = 600) AS s6, (SELECT count() AS h7 FROM t WHERE v > 10 AND k = 700) AS s7, (SELECT count() AS h8 FROM t WHERE v > 10 AND k = 800) AS s8 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '18 countIf', countIf(explain LIKE '%countIf%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS h1 FROM t WHERE v > 10 AND k = 100) AS s1, (SELECT count() AS h2 FROM t WHERE v > 10 AND k = 200) AS s2, (SELECT count() AS h3 FROM t WHERE v > 10 AND k = 300) AS s3, (SELECT count() AS h4 FROM t WHERE v > 10 AND k = 400) AS s4, (SELECT count() AS h5 FROM t WHERE v > 10 AND k = 500) AS s5, (SELECT count() AS h6 FROM t WHERE v > 10 AND k = 600) AS s6, (SELECT count() AS h7 FROM t WHERE v > 10 AND k = 700) AS s7, (SELECT count() AS h8 FROM t WHERE v > 10 AND k = 800) AS s8 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 19 an ARRAY JOIN is not a table expression the pass can substitute positionally';
SELECT '19', c1, c2 FROM (SELECT count() AS c1 FROM arr ARRAY JOIN a AS e WHERE k = 1) AS x, (SELECT count() AS c2 FROM arr ARRAY JOIN a AS e WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '19', c1, c2 FROM (SELECT count() AS c1 FROM arr ARRAY JOIN a AS e WHERE k = 1) AS x, (SELECT count() AS c2 FROM arr ARRAY JOIN a AS e WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '19 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS c1 FROM arr ARRAY JOIN a AS e WHERE k = 1) AS x, (SELECT count() AS c2 FROM arr ARRAY JOIN a AS e WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 20 only comparisons and logical connectives are accepted; a Decimal compared with an integer literal is rescaled, which can overflow, while the same comparison at equal scale is accepted';
SELECT '20a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND bitAnd(k, 3) = 1) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '20b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM dec WHERE id > 0 AND p BETWEEN 8 AND 18) AS x, (SELECT count() AS b FROM dec WHERE id > 0 AND p BETWEEN 20 AND 30) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '20c', a, b FROM (SELECT count() AS a FROM dec WHERE id > 0 AND p > toDecimal32(8, 2)) AS x, (SELECT count() AS b FROM dec WHERE id > 0 AND p > toDecimal32(20, 2)) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '20c', a, b FROM (SELECT count() AS a FROM dec WHERE id > 0 AND p > toDecimal32(8, 2)) AS x, (SELECT count() AS b FROM dec WHERE id > 0 AND p > toDecimal32(20, 2)) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '20c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM dec WHERE id > 0 AND p > toDecimal32(8, 2)) AS x, (SELECT count() AS b FROM dec WHERE id > 0 AND p > toDecimal32(20, 2)) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 21 two occurrences of one table compare equal, so a conjunct binding p is matched against one binding q and dropped as shared; branch y answers 0 only while the occurrences stay distinct';
SELECT '21', a, b FROM (SELECT count() AS a FROM m AS p, m AS q WHERE p.k = 1 AND q.k > 5) AS x, (SELECT count() AS b FROM m AS p, m AS q WHERE q.k = 1 AND q.k > 7) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '21', a, b FROM (SELECT count() AS a FROM m AS p, m AS q WHERE p.k = 1 AND q.k > 5) AS x, (SELECT count() AS b FROM m AS p, m AS q WHERE q.k = 1 AND q.k > 7) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '21 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m AS p, m AS q WHERE p.k = 1 AND q.k > 5) AS x, (SELECT count() AS b FROM m AS p, m AS q WHERE q.k = 1 AND q.k > 7) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 22 a column whose value is produced by a DEFAULT expression is materialized for more rows once the residual moves inside the OR';
SELECT '22', a, b FROM (SELECT count() AS a FROM dflt WHERE k > 0 AND x > 5) AS x, (SELECT count() AS b FROM dflt WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '22', a, b FROM (SELECT count() AS a FROM dflt WHERE k > 0 AND x > 5) AS x, (SELECT count() AS b FROM dflt WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '22 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM dflt WHERE k > 0 AND x > 5) AS x, (SELECT count() AS b FROM dflt WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 23 additional_table_filters is resolved per table expression in the planner, after this pass has run';
SELECT '23', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, additional_table_filters = {'t': 'k < 400'};
SELECT '23', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, additional_table_filters = {'t': 'k < 400'};
SELECT '23 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, additional_table_filters = {'t': 'k < 400'});

SELECT '-- 24 a row policy is an expression this pass never sees';
DROP ROW POLICY IF EXISTS rp_05218 ON rp;
CREATE ROW POLICY rp_05218 ON rp USING k < 400 TO ALL;
SELECT '24', a, b FROM (SELECT count() AS a FROM rp WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rp WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '24', a, b FROM (SELECT count() AS a FROM rp WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rp WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '24 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM rp WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM rp WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
DROP ROW POLICY rp_05218 ON rp;

SELECT '-- 25 sibling branches must read one snapshot of the table, not one each';
SELECT '25a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, enable_shared_storage_snapshot_in_query = 0);
SELECT '25b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, enable_shared_storage_snapshot_in_query = 1);

SELECT '-- 26 residuals over two tables lose their per-branch correlation inside the fused OR; over one table they do not';
SELECT '26a', a, b FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 1 AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 2 AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '26a', a, b FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 1 AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 2 AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '26a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 1 AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f1.x = 2 AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '26b', a, b FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '26b', a, b FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '26b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 1) AS x, (SELECT count() AS b FROM f1, f2 WHERE f1.g = f2.g AND f2.x = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 27 a projection-alias override whose size would stop matching the grown projection';
SELECT '27', a, b FROM (SELECT count() FROM t WHERE v > 10 AND k = 300) AS x(a), (SELECT count() FROM t WHERE v > 10 AND k = 500) AS y(b) SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '27', a, b FROM (SELECT count() FROM t WHERE v > 10 AND k = 300) AS x(a), (SELECT count() FROM t WHERE v > 10 AND k = 500) AS y(b) SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '27 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT a, b FROM (SELECT count() FROM t WHERE v > 10 AND k = 300) AS x(a), (SELECT count() FROM t WHERE v > 10 AND k = 500) AS y(b) SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

DROP TABLE t;
DROP TABLE tn;
DROP TABLE m;
DROP TABLE lo;
DROP TABLE dflt;
DROP TABLE dec;
DROP TABLE f1;
DROP TABLE f2;
DROP TABLE arr;
DROP TABLE al;
DROP TABLE rmt;
DROP TABLE rp;
