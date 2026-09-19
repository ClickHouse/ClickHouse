SET enable_analyzer = 1; -- FuseSiblingAggregateSubqueriesPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer
-- The fused branches must read one snapshot of each table, so the pass refuses when sibling branches
-- can capture their own. CI randomizes this setting, and arm 25 toggles it deliberately.
SET enable_shared_storage_snapshot_in_query = 1;
-- The pass refuses whenever any of these is set at all, whatever the value, and the stateless test
-- profile (tests/config/users.d/limits.yaml) sets eight of them in the default profile, so without
-- this every arm asserting a rewrite would assert one that cannot happen. Arms 29a/29b/29c set their
-- own in query-level SETTINGS, which override this.
SET max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0, max_columns_to_read = 0, max_temporary_columns = 0, max_temporary_non_const_columns = 0, max_rows_in_join = 0, max_bytes_in_join = 0;

-- Arms 01 to 22 are in 05218_fuse_sibling_aggregate_subqueries.sql; each arm prints its answer with
-- the optimization off and then on (the two lines must agree), plus a query-tree assertion,
-- because equal answers alone cannot tell a correct rewrite from no rewrite.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS f1;
DROP TABLE IF EXISTS f2;
DROP TABLE IF EXISTS rp;
DROP TABLE IF EXISTS jl;
DROP TABLE IF EXISTS jr;
DROP TABLE IF EXISTS gl;
DROP TABLE IF EXISTS gr;
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

-- f2.x is independent of g, so arm 26a's f2.x residual is not implied by the join equality and a
-- dropped residual changes the answer (120 rather than 800).
CREATE TABLE f1 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f1 SELECT number % 10, number % 5 FROM numbers(200);

CREATE TABLE f2 (g Int64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f2 SELECT number % 10, intDiv(number, 7) % 5 FROM numbers(200);

CREATE TABLE rp (k Int64, v Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO rp SELECT number, number * 2 FROM numbers(1000);

-- Small granules, so that 120 joined rows cross max_rows_in_join = 100 mid-block.
CREATE TABLE jl (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO jl SELECT number FROM numbers(120);

CREATE TABLE jr (id UInt64, bucket UInt8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 10;
INSERT INTO jr SELECT number, number % 2 FROM numbers(120);

-- Sixteen buckets of 1000 rows, of which arm 37 fuses eight: one branch builds 1000 right rows and
-- the fused OR builds 8000, while buckets 8 to 15 keep that OR a proper subset of the table.
CREATE TABLE gl (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO gl SELECT number FROM numbers(16000);

CREATE TABLE gr (id UInt64, bucket UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO gr SELECT number, number % 16 FROM numbers(16000);

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

SELECT '-- 37 the fused residual widens the join build side, and a grace hash join raises rather than pass grace_hash_join_max_buckets';
-- The two arms differ in nothing but the pass. max_bytes_before_external_join is the geometric mean of
-- the two measured switch boundaries (77258 bytes for one branch, 334264 for the fused eight), so each
-- arm has a factor of two of headroom; the settings that move those byte counts are pinned because CI
-- randomizes them. max_bytes_ratio_before_external_join feeds the same threshold and is defaulted on,
-- which is what makes this reachable without setting the absolute one.
SELECT '37', c0, c1, c2, c3, c4, c5, c6, c7 FROM (SELECT count() AS c0 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 0) AS t0, (SELECT count() AS c1 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 1) AS t1, (SELECT count() AS c2 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 2) AS t2, (SELECT count() AS c3 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 3) AS t3, (SELECT count() AS c4 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 4) AS t4, (SELECT count() AS c5 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 5) AS t5, (SELECT count() AS c6 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 6) AS t6, (SELECT count() AS c7 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 7) AS t7 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 0, join_algorithm = 'hash', max_bytes_before_external_join = 160000, max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1, query_plan_join_swap_table = false, max_threads = 1, max_block_size = 65505, max_joined_block_size_rows = 65505;
SELECT '37', c0, c1, c2, c3, c4, c5, c6, c7 FROM (SELECT count() AS c0 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 0) AS t0, (SELECT count() AS c1 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 1) AS t1, (SELECT count() AS c2 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 2) AS t2, (SELECT count() AS c3 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 3) AS t3, (SELECT count() AS c4 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 4) AS t4, (SELECT count() AS c5 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 5) AS t5, (SELECT count() AS c6 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 6) AS t6, (SELECT count() AS c7 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 7) AS t7 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1, join_algorithm = 'hash', max_bytes_before_external_join = 160000, max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 1, grace_hash_join_max_buckets = 1, query_plan_join_swap_table = false, max_threads = 1, max_block_size = 65505, max_joined_block_size_rows = 65505; -- { serverError LIMIT_EXCEEDED }
SELECT '37 fused', countIf(explain LIKE '%countIf%') FROM (EXPLAIN QUERY TREE SELECT * FROM (SELECT count() AS c0 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 0) AS t0, (SELECT count() AS c1 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 1) AS t1, (SELECT count() AS c2 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 2) AS t2, (SELECT count() AS c3 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 3) AS t3, (SELECT count() AS c4 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 4) AS t4, (SELECT count() AS c5 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 5) AS t5, (SELECT count() AS c6 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 6) AS t6, (SELECT count() AS c7 FROM gl AS l, gr AS r WHERE l.id = r.id AND r.bucket = 7) AS t7 SETTINGS optimize_fuse_sibling_aggregate_subqueries = 1);

DROP TABLE t;
DROP TABLE f1;
DROP TABLE f2;
DROP TABLE rp;
DROP TABLE jl;
DROP TABLE jr;
DROP TABLE gl;
DROP TABLE gr;
DROP TABLE cols;
DROP TABLE proj;
DROP TABLE smp;
DROP TABLE pq;
DROP TABLE pt;
DROP TABLE mm;
DROP TABLE cq;
DROP TABLE cqn;
