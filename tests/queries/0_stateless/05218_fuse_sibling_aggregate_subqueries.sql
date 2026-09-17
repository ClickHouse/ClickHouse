SET enable_analyzer = 1; -- FuseSiblingAggregateSubqueriesPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer
-- The fused branches must read one snapshot of each table, so the pass refuses when sibling branches
-- can capture their own. CI randomizes this setting, and arm 25 toggles it deliberately.
SET enable_shared_storage_snapshot_in_query = 1;
-- The pass refuses whenever any of these is set at all, whatever the value, and the stateless test
-- profile (tests/config/users.d/limits.yaml) sets eight of them in the default profile, so without
-- this every arm asserting a rewrite would assert one that cannot happen. Arms 29a/29b/29c set their
-- own in query-level SETTINGS, which override this.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0, max_columns_to_read = 0, max_temporary_columns = 0, max_temporary_non_const_columns = 0, max_rows_in_join = 0, max_bytes_in_join = 0;

-- Each arm prints its answer with the optimization off and then on (the two lines must agree), plus a
-- query-tree assertion, because equal answers alone cannot tell a correct rewrite from no rewrite.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS tn;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS lo;
DROP TABLE IF EXISTS dyn;
DROP TABLE IF EXISTS dflt;
DROP TABLE IF EXISTS dec;
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS arr;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS rmt;
DROP TABLE IF EXISTS rp;
DROP TABLE IF EXISTS jl;
DROP TABLE IF EXISTS jr;
DROP TABLE IF EXISTS cols;
DROP TABLE IF EXISTS proj;
DROP TABLE IF EXISTS smp;
DROP TABLE IF EXISTS pq;
DROP TABLE IF EXISTS pt;
DROP TABLE IF EXISTS mm;
DROP TABLE IF EXISTS cq;
DROP TABLE IF EXISTS cqn;

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
INSERT INTO dflt VALUES (0), (1), (2);
ALTER TABLE dflt ADD COLUMN x Int64 DEFAULT k * 10;

CREATE TABLE dec (id Int64, p Decimal(7, 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO dec SELECT number, number FROM numbers(100);

-- f2.x is independent of g, so arm 26a's f2.x residual is not implied by the join equality and a
-- dropped residual changes the answer (120 rather than 800).
CREATE TABLE f1 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f1 SELECT number % 10, number % 5 FROM numbers(200);
CREATE TABLE f2 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f2 SELECT number % 10, intDiv(number, 7) % 5 FROM numbers(200);

CREATE TABLE arr (k Int64, a Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO arr VALUES (1, [1, 2]), (2, [3, 4]);

CREATE TABLE al (k Int64, y Int64 ALIAS k + 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO al (k) SELECT number FROM numbers(100);

CREATE TABLE rmt (k Int64, v Int64) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO rmt SELECT number, number * 2 FROM numbers(1000);

CREATE TABLE rp (k Int64, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rp SELECT number, number * 2 FROM numbers(1000);

-- Small granules, so that 120 joined rows cross max_rows_in_join = 100 mid-block.
CREATE TABLE jl (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO jl SELECT number FROM numbers(120);
CREATE TABLE jr (id UInt64, bucket UInt8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO jr SELECT number, number % 2 FROM numbers(120);

CREATE TABLE cols (c1 UInt8, c2 UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO cols SELECT number % 2, number % 3 FROM numbers(100);

CREATE TABLE proj (k UInt8, PROJECTION p0 (SELECT count() WHERE k = 0), PROJECTION p1 (SELECT count() WHERE k = 1))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO proj SELECT number % 4 FROM numbers(1000);

CREATE TABLE smp (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SAMPLE BY k;
INSERT INTO smp SELECT number, number FROM numbers(100000);

-- Two partitions, so each branch's own read spans one and the fused read spans both.
CREATE TABLE pq (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO pq SELECT number % 2, number FROM numbers(100);

-- The same shape with the limit carried by the table rather than by the query.
CREATE TABLE pt (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS max_partitions_to_read = 1;
INSERT INTO pt SELECT number % 2, number FROM numbers(100);

-- No explicit projection: each branch's count() over a partition filter is answered by the implicit
-- _minmax_count_projection, which the -If rewrite makes ineligible.
CREATE TABLE mm (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO mm SELECT number % 4, number FROM numbers(1000);

-- The mark gate on the table concurrency limit, and the same shape without it. Only whether both
-- settings are set is what the guard reads, so max_concurrent_queries is deliberately far above the
-- number of readers any test flavor opens on one table: at 1, a flavor that reads through several
-- replicas spends the limit on this fixture's own queries.
CREATE TABLE cq (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS max_concurrent_queries = 100, min_marks_to_honor_max_concurrent_queries = 1;
INSERT INTO cq SELECT number % 2, number FROM numbers(10);
CREATE TABLE cqn (p UInt8, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k;
INSERT INTO cqn SELECT number % 2, number FROM numbers(10);

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

SELECT '-- 28 a branch with no WHERE of its own is refused: it is already read in full, or answered from part metadata, and it would leave the fused filter empty';
SELECT '28a', a, b FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '28a', a, b FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '28a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- Refusing that branch leaves its filtered siblings free to fuse with each other, which is why the
-- refusal belongs to the branch rather than to the group.
SELECT '28b', a, b, c FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 300) AS y, (SELECT count() AS c FROM t WHERE v > 10 AND k = 500) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '28b', a, b, c FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 300) AS y, (SELECT count() AS c FROM t WHERE v > 10 AND k = 500) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '28b countIf', countIf(explain LIKE '%countIf%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 300) AS y, (SELECT count() AS c FROM t WHERE v > 10 AND k = 500) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '28b tables', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 300) AS y, (SELECT count() AS c FROM t WHERE v > 10 AND k = 500) AS z SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

-- query_plan_join_swap_table decides which side of the join is built, which moves where
-- join_overflow_mode = 'break' truncates: with it left at the default `auto` the truncated count is
-- 60 or 50 independently of this pass (measured: 12 of 50 randomized runs), so it is pinned here.
SELECT '-- 29 a limit that bounds the whole query is evaluated on the fused shape, where N bounded reads and joins have become one';
SELECT '29a', x.a, y.b FROM (SELECT count() AS a FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 0) AS x, (SELECT count() AS b FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, join_algorithm = 'hash', max_rows_in_join = 100, join_overflow_mode = 'break', max_threads = 1, max_block_size = 10, query_plan_join_swap_table = false;
SELECT '29a', x.a, y.b FROM (SELECT count() AS a FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 0) AS x, (SELECT count() AS b FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, join_algorithm = 'hash', max_rows_in_join = 100, join_overflow_mode = 'break', max_threads = 1, max_block_size = 10, query_plan_join_swap_table = false;
SELECT '29a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 0) AS x, (SELECT count() AS b FROM jl AS l, jr AS r WHERE l.id = r.id AND r.bucket = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, join_algorithm = 'hash', max_rows_in_join = 100, join_overflow_mode = 'break', max_threads = 1, max_block_size = 10, query_plan_join_swap_table = false);
-- Two one-column reads become one two-column read, so the answer itself is what is asserted here:
-- both arms must succeed.
SELECT '29b', x.a, y.b FROM (SELECT count() AS a FROM cols WHERE c1 = 0) AS x, (SELECT count() AS b FROM cols WHERE c2 = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, max_columns_to_read = 1;
SELECT '29b', x.a, y.b FROM (SELECT count() AS a FROM cols WHERE c1 = 0) AS x, (SELECT count() AS b FROM cols WHERE c2 = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_columns_to_read = 1;
SELECT '29b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM cols WHERE c1 = 0) AS x, (SELECT count() AS b FROM cols WHERE c2 = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_columns_to_read = 1);
-- The rest of the family, each measured to turn a query that succeeds into one that fails once
-- fused (evidence.md has the codes); asserted as a refusal, because a row-count bound tight
-- enough to separate the two arms would move with the randomized index granularity.
SELECT '29c max_rows_to_read', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_rows_to_read = 1000000);
SELECT '29c max_bytes_to_read', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_bytes_to_read = 1000000);
SELECT '29c max_rows_to_read_leaf', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_rows_to_read_leaf = 1000000);
SELECT '29c max_bytes_to_read_leaf', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_bytes_to_read_leaf = 1000000);
SELECT '29c max_temporary_columns', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_temporary_columns = 33);
SELECT '29c max_temporary_non_const_columns', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_temporary_non_const_columns = 33);
SELECT '29c max_bytes_in_join', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_bytes_in_join = 1000000);
SELECT '29c none', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 30 a projection is chosen against the own filter of a branch, and the fused OR implies none of them';
SELECT '30', x.a, y.b FROM (SELECT count() AS a FROM proj WHERE k = 0) AS x, (SELECT count() AS b FROM proj WHERE k = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, force_optimize_projection = 1;
SELECT '30', x.a, y.b FROM (SELECT count() AS a FROM proj WHERE k = 0) AS x, (SELECT count() AS b FROM proj WHERE k = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, force_optimize_projection = 1;
SELECT '30 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM proj WHERE k = 0) AS x, (SELECT count() AS b FROM proj WHERE k = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, force_optimize_projection = 1);

SELECT '-- 31 for an absolute SAMPLE the sampled set is derived from the own key condition of a branch, which fusion replaces with the merged one, so any modifier is refused';
SELECT '31a', x.a, y.b FROM (SELECT count() AS a FROM smp SAMPLE 1000 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 1000 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '31a', x.a, y.b FROM (SELECT count() AS a FROM smp SAMPLE 1000 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 1000 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '31a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM smp SAMPLE 1000 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 1000 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
SELECT '31b', x.a, y.b FROM (SELECT count() AS a FROM smp SAMPLE 0.1 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 0.1 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '31b', x.a, y.b FROM (SELECT count() AS a FROM smp SAMPLE 0.1 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 0.1 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '31b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM smp SAMPLE 0.1 WHERE v > 0 AND k < 1000) AS x, (SELECT count() AS b FROM smp SAMPLE 0.1 WHERE v > 0 AND k >= 99000) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 32 a limit on how far one read may span is evaluated on the fused read, which spans the union of the partitions the branches read';
SELECT '32a', a, b FROM (SELECT count() AS a FROM pq WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pq WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, max_partitions_to_read = 1;
SELECT '32a', a, b FROM (SELECT count() AS a FROM pq WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pq WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_partitions_to_read = 1;
SELECT '32a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM pq WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pq WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, max_partitions_to_read = 1);
-- The effective limit is the table's own whenever the query has not set one, which is a fact about the
-- data rather than about the query, so the pass has to ask the storage for it.
SELECT '32b', a, b FROM (SELECT count() AS a FROM pt WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pt WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '32b', a, b FROM (SELECT count() AS a FROM pt WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pt WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '32b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM pt WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pt WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- Neither spelling in force: the same shape fuses, so the two refusals above are the limit and not the shape.
SELECT '32c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM pq WHERE k < 100 AND p = 0) AS x, (SELECT count() AS b FROM pq WHERE k < 100 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 33 the implicit minmax_count projection is a separate member of the metadata, so a table with no projection of its own still loses an access path the rewrite cannot keep';
SELECT '33a', a, b FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection = 1;
SELECT '33a', a, b FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection = 1;
SELECT '33a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection = 1);
SELECT '33b', a, b FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection_name = '_minmax_count_projection';
SELECT '33b', a, b FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection_name = '_minmax_count_projection';
SELECT '33b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, force_optimize_projection_name = '_minmax_count_projection');
-- Nothing forced: the same shape fuses, and the projection it silently gives up is disclosed rather than guarded.
SELECT '33c fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM mm WHERE p = 0) AS x, (SELECT count() AS b FROM mm WHERE p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1);

SELECT '-- 34 the same limit also gates a table-wide concurrency slot on the marks one read selects, and the fused read selects the marks of the union, so it can have to take a slot neither branch read needed';
SELECT '34a', a, b FROM (SELECT count() AS a FROM cq WHERE k < 10 AND p = 0) AS x, (SELECT count() AS b FROM cq WHERE k < 10 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '34a', a, b FROM (SELECT count() AS a FROM cq WHERE k < 10 AND p = 0) AS x, (SELECT count() AS b FROM cq WHERE k < 10 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
SELECT '34a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM cq WHERE k < 10 AND p = 0) AS x, (SELECT count() AS b FROM cq WHERE k < 10 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);
-- Neither setting on the table: the same shape fuses, so 34a's refusal is the settings and not the shape.
SELECT '34b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM cqn WHERE k < 10 AND p = 0) AS x, (SELECT count() AS b FROM cqn WHERE k < 10 AND p = 1) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 35 a branch aggregate whose own name is at the factory length limit: appending If exceeds it, and the factory answers an over-long name by throwing rather than by not resolving';
SELECT '35', finalizeAggregation(a), finalizeAggregation(b) FROM (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS a FROM t WHERE k = 300) AS x, (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS b FROM t WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0;
SELECT '35', finalizeAggregation(a), finalizeAggregation(b) FROM (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS a FROM t WHERE k = 300) AS x, (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS b FROM t WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1;
-- The name the rewrite would have built cannot appear in any probe that names a function, so the
-- table expressions are what says the shape was refused.
SELECT '35 tables', countIf(explain LIKE '%TABLE id:%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS a FROM t WHERE k = 300) AS x, (SELECT countStateOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefaultOrDefault() AS b FROM t WHERE k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

SELECT '-- 36 cross_to_inner_join_rewrite = 2 rejects a comma join it cannot turn into an INNER JOIN, and it is reached only while the join tree is still a cross join';
-- Both arms must raise: the rewrite must not answer a query the setting rejects.
SELECT a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y WHERE a + b > 0 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }
SELECT a, b FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y WHERE a + b > 0 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, cross_to_inner_join_rewrite = 2; -- { serverError INCORRECT_QUERY }
-- The rejection needs a WHERE to reach, so the refusal itself is asserted on a shape that raises in
-- neither arm, and 36a/36b are what say it is the setting's value and the comma and not the shape.
SELECT '36 fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, cross_to_inner_join_rewrite = 2);
SELECT '36a fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x, (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, cross_to_inner_join_rewrite = 1);
SELECT '36b fused', countIf(explain LIKE '%countIf%') > 0 FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS a FROM t WHERE v > 10 AND k = 300) AS x CROSS JOIN (SELECT count() AS b FROM t WHERE v > 10 AND k = 500) AS y SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, cross_to_inner_join_rewrite = 2);

DROP TABLE t;
DROP TABLE tn;
DROP TABLE m;
DROP TABLE lo;
DROP TABLE dyn;
DROP TABLE dflt;
DROP TABLE dec;
DROP TABLE f1;
DROP TABLE f2;
DROP TABLE arr;
DROP TABLE al;
DROP TABLE rmt;
DROP TABLE rp;
DROP TABLE jl;
DROP TABLE jr;
DROP TABLE cols;
DROP TABLE proj;
DROP TABLE smp;
DROP TABLE pq;
DROP TABLE pt;
DROP TABLE mm;
DROP TABLE cq;
DROP TABLE cqn;
