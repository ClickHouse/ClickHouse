SET enable_analyzer = 1; -- FuseSiblingAggregateSubqueriesPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer
-- The fused branches must read one snapshot of each table, so the pass refuses when sibling branches
-- can capture their own, and CI randomizes this setting.
SET enable_shared_storage_snapshot_in_query = 1;
-- The pass refuses whenever any of these is set at all, whatever the value, and the stateless test
-- profile (tests/config/users.d/limits.yaml) sets eight of them in the default profile, so without
-- this every arm asserting a rewrite would assert one that cannot happen.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0, max_columns_to_read = 0, max_temporary_columns = 0, max_temporary_non_const_columns = 0, max_rows_in_join = 0, max_bytes_in_join = 0;

-- Each arm prints its answer with the optimization off and then on (the two lines must agree), plus a
-- query-tree assertion, because equal answers alone cannot tell a correct rewrite from no rewrite.
-- Arms 23 to 37 are in 05218_fuse_sibling_aggregate_subqueries_2.sql: one file running every
-- arm exceeds the flaky check's 180 s per-run limit, which that job reaches by running many
-- copies of the same test at once.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tn;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS lo;
DROP TABLE IF EXISTS dyn;
DROP TABLE IF EXISTS dflt;
DROP TABLE IF EXISTS dec;
DROP TABLE IF EXISTS arr;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS rmt;

CREATE TABLE t (k Int64, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, number * 2 FROM numbers(1000);

CREATE TABLE tn (k Nullable(Int64), v Int64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tn SELECT number, number * 2, toString(number % 10) FROM numbers(1000);

-- k is not the sorting key, so no primary-key range pruning can rescue a throwing conjunct.
CREATE TABLE m (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO m SELECT number, number FROM numbers(10);

-- Holds the Int64 minimum, the value intDiv(k, -1) overflows on.
CREATE TABLE lo (id Int64, k Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lo SELECT * FROM values('id Int64, k Int64', (1, -9223372036854775808), (2, 5), (3, 7));

-- d1 = d2 compares the tuples element by element, so the Dynamic elements dispatch on each row's own
-- types and the p = 1 row, whose elements are a String and an integer, has no supertype to compare in.
-- ORDER BY tuple() keeps primary-key pruning from dropping that row from the fused read; PARTITION BY p
-- keeps it out of the unfused branch's read whatever the plan does with that branch's AND, so arm 03d
-- measures the pass rather than filter push-down.
CREATE TABLE dyn (p UInt8, d1 Tuple(Dynamic), d2 Tuple(Dynamic)) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO dyn VALUES (0, tuple(42), tuple(42)), (1, tuple('x'), tuple(42));

-- x is added after the parts are written, so the parts do not store it and reading a row evaluates its
-- DEFAULT expression inside the reader.
CREATE TABLE dflt (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dflt SELECT * FROM values('k Int64', 0, 1, 2);
ALTER TABLE dflt ADD COLUMN x Int64 DEFAULT k * 10;

CREATE TABLE dec (id Int64, p Decimal(7, 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO dec SELECT number, number FROM numbers(100);

CREATE TABLE arr (k Int64, a Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO arr SELECT * FROM values('k Int64, a Array(Int64)', (1, [1, 2]), (2, [3, 4]));

CREATE TABLE al (k Int64, y Int64 ALIAS k + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO al (k) SELECT number FROM numbers(100);

CREATE TABLE rmt (k Int64, v Int64) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO rmt SELECT number, number * 2 FROM numbers(1000);

SELECT '-- 01 two sibling branches over one table: fuses';
SELECT '01', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '01', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '01 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- Inserting the -If aggregates and removing the fused-away branches are separate steps, so the number
-- of table expressions is asserted as well: without it, a rewrite that kept every scan would pass.
SELECT '01 tables off', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0);
SELECT '01 tables on', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

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

-- 03d executes its answers, unlike 03/03b/03c: the throwing row is in a partition the unfused branch
-- prunes. A comparison of two equal types reads the values as they are stored and cannot throw, which
-- is why Variant/Dynamic/JSON are excluded from that rule, but only at the top level: nested in a tuple
-- they still dispatch per row. The sweep is on the answer with the optimization on because
-- force_enable defers the residual and would answer even if the pass wrongly fused.
SELECT '-- 03d a Dynamic nested in a tuple dispatches on each row own types, so it is refused';
SELECT '03d', a, b FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '03d', a, b FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '03d disable', a, b FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'disable';
SELECT '03d enable', a, b FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'enable';
SELECT '03d force', a, b FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, short_circuit_function_evaluation = 'force_enable';
SELECT '03d fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM dyn WHERE p = 0 AND d1 = d2) AS x, (SELECT count() AS b FROM dyn WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 04 a branch whose conjuncts are all shared has no -If and keeps every row the shared part keeps';
SELECT '04', a, b FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '04', a, b FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '04 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 05 a filter that is not reproducible keeps its own scan';
SELECT '05 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND rand() < 100) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '06a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND rowNumberInAllBlocks() < 5) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '06b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM m WHERE k > 8 AND sleepEachRow(0.001) = 0) AS x, (SELECT count() AS b FROM m WHERE k = 2) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 07 the analyzer folds now() into a constant before the reproducibility check can see it, so it does not block fusion';
SELECT '07', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '07', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '07 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND toDateTime('2020-01-01') < now()) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 08 a NULL -If condition excludes its row exactly as a NULL WHERE does';
SELECT '08', a, b FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '08', a, b FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '08 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM tn WHERE k = 300) AS x, (SELECT count() AS b FROM tn WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 09 an empty branch, and the same for an argument-less aggregate that carries a combinator suffix';
SELECT '09a', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '09a', a, b FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '09a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE k = 300) AS x, (SELECT count() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- The suffix reaches the fused aggregate too, whose name is then countOrNullIf, so the countIf probe
-- would be blind here: the suffixed name and the table expressions are asserted instead.
SELECT '09b', a, b FROM (SELECT countOrNull() AS a FROM t WHERE k = 300) AS x, (SELECT countOrNull() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '09b', a, b FROM (SELECT countOrNull() AS a FROM t WHERE k = 300) AS x, (SELECT countOrNull() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '09b fused', countIf(explain LIKE '%countOrNullIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT countOrNull() AS a FROM t WHERE k = 300) AS x, (SELECT countOrNull() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '09b tables off', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT countOrNull() AS a FROM t WHERE k = 300) AS x, (SELECT countOrNull() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0);
SELECT '09b tables on', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT countOrNull() AS a FROM t WHERE k = 300) AS x, (SELECT countOrNull() AS b FROM t WHERE k = -1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 10 with empty_result_for_aggregation_by_empty_set an empty branch empties the whole join, which one fused aggregation cannot reproduce';
SELECT '10', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, empty_result_for_aggregation_by_empty_set = 1;
SELECT '10', a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, empty_result_for_aggregation_by_empty_set = 1;
SELECT '10 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, empty_result_for_aggregation_by_empty_set = 1);

SELECT '-- 11 an aggregate with an argument is out of scope: the -If condition would stop protecting it';
SELECT '11a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(DISTINCT k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(DISTINCT k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- The rewrite names the fused aggregate after the branch's own function, so these two would emit
-- uniqExactIf and avgIf rather than countIf: the table expressions are counted as well.
SELECT '11b tables', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count(DISTINCT k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count(DISTINCT k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT avg(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT avg(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '11c tables', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT avg(k) AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT avg(k) AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
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

SELECT '-- 15 a table expression that carries any modifier is refused: what the modifier makes the read return is not preserved by the fused filter (arm 31), and branches differing in FINAL are not siblings to begin with';
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
SELECT '18 tables off', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS h1 FROM t WHERE v > 10 AND k = 100) AS s1, (SELECT count() AS h2 FROM t WHERE v > 10 AND k = 200) AS s2, (SELECT count() AS h3 FROM t WHERE v > 10 AND k = 300) AS s3, (SELECT count() AS h4 FROM t WHERE v > 10 AND k = 400) AS s4, (SELECT count() AS h5 FROM t WHERE v > 10 AND k = 500) AS s5, (SELECT count() AS h6 FROM t WHERE v > 10 AND k = 600) AS s6, (SELECT count() AS h7 FROM t WHERE v > 10 AND k = 700) AS s7, (SELECT count() AS h8 FROM t WHERE v > 10 AND k = 800) AS s8 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0);
SELECT '18 tables on', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS h1 FROM t WHERE v > 10 AND k = 100) AS s1, (SELECT count() AS h2 FROM t WHERE v > 10 AND k = 200) AS s2, (SELECT count() AS h3 FROM t WHERE v > 10 AND k = 300) AS s3, (SELECT count() AS h4 FROM t WHERE v > 10 AND k = 400) AS s4, (SELECT count() AS h5 FROM t WHERE v > 10 AND k = 500) AS s5, (SELECT count() AS h6 FROM t WHERE v > 10 AND k = 600) AS s6, (SELECT count() AS h7 FROM t WHERE v > 10 AND k = 700) AS s7, (SELECT count() AS h8 FROM t WHERE v > 10 AND k = 800) AS s8 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

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

DROP TABLE t;
DROP TABLE tn;
DROP TABLE m;
DROP TABLE lo;
DROP TABLE dyn;
DROP TABLE dflt;
DROP TABLE dec;
DROP TABLE arr;
DROP TABLE al;
DROP TABLE rmt;
