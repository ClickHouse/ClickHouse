-- Join reordering estimates an OR with an equality in every branch as a join on those equalities where the join runs on
-- them, and a join of such an OR with other conditions is not a cross join (issue #122511).

SET enable_parallel_replicas = 0;
SET allow_general_join_planning = 1, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false', use_hash_table_stats_for_join_reordering = 0, query_plan_join_shard_by_pk_ranges = 0;
SET use_statistics = 1, use_statistics_cache = 0, materialize_statistics_on_insert = 1;

-- The issue's reproducer with ten times the rows: at the issue's size the two candidate first joins cost the same.
CREATE TABLE t1 (c1 UUID, c2 UUID, c3 String, c4 String, c5 String, c6 DateTime) ENGINE = ReplacingMergeTree(c6) ORDER BY (c2, c1) SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t2 (c1 UUID, c2 UUID, c3 UUID, c4 DateTime, c5 Nullable(DateTime), c6 DateTime) ENGINE = ReplacingMergeTree(c6) ORDER BY (c1, c2, c3, c4) SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t1 SELECT reinterpretAsUUID(sipHash128(number)), toUUID('00000000-0000-0000-0000-000000000001'), toString(number), if(number % 2 = 0, 'a', 'b'), '', toDateTime('2026-01-01 00:00:00') FROM numbers(10000);
INSERT INTO t2 SELECT toUUID('00000000-0000-0000-0000-000000000001'), x.c1, y.c1, toDateTime('2026-01-01 00:00:00'), NULL, toDateTime('2026-01-01 00:00:00') FROM (SELECT c1, row_number() OVER (ORDER BY c1) AS r FROM t1) AS x INNER JOIN (SELECT c1, row_number() OVER (ORDER BY c1 DESC) AS r FROM t1) AS y ON x.r = y.r WHERE x.r <= 5000;

SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';

SET query_plan_optimize_join_order_algorithm = 'dpsize';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';

SELECT count(), sum(cnt), sum(cityHash64(x, y)) FROM (
WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) AS s (x, y, cnt);

SET query_plan_optimize_join_order_algorithm = 'dpsub';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';

-- Without statistics.
SET query_plan_optimize_join_order_algorithm = 'greedy', use_statistics = 0;
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
SET use_statistics = 1;

-- An OR that ends up in one join with conditions of another JOIN ON clause.
CREATE TABLE ta (x UInt64, y UInt64, v UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE tb (x UInt64, y UInt64, k UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE tc (k UInt64, z UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO ta SELECT number, number + 1, number % 2 FROM numbers(10000);
INSERT INTO tb SELECT number, number + 1, number FROM numbers(10000);
INSERT INTO tc SELECT number, number + number % 3 FROM numbers(10);

SET query_plan_optimize_join_order_algorithm = 'greedy';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM ta JOIN tb ON ta.x = tb.x OR ta.y = tb.y JOIN tc ON tb.k = tc.k AND tc.z = ta.v + tb.k AND ta.y > 1
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
SELECT count() FROM ta JOIN tb ON ta.x = tb.x OR ta.y = tb.y JOIN tc ON tb.k = tc.k AND tc.z = ta.v + tb.k AND ta.y > 1;

-- A join with two ORs runs as a cross join, so it must be estimated as one.
CREATE TABLE ra (x UInt64, y UInt64, w UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE rb (x UInt64, y UInt64, u UInt64, v UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE rc (u UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY u SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO ra SELECT number, number + 1, number FROM numbers(1000);
INSERT INTO rb SELECT number, number + 1, number, number + 1 FROM numbers(1000);
INSERT INTO rc SELECT number, number + 1, number FROM numbers(1000);

SET query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_merge_filter_into_join_condition = 1;
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM ra JOIN rb ON ra.x = rb.x OR ra.y = rb.y JOIN rc ON rc.u = rb.u OR rc.v = rb.v WHERE rc.w = ra.w
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
SELECT count() FROM ra JOIN rb ON ra.x = rb.x OR ra.y = rb.y JOIN rc ON rc.u = rb.u OR rc.v = rb.v WHERE rc.w = ra.w;

-- An OR in a join that has another key: the join uses that key and applies the OR as a filter.
CREATE TABLE s0 (x UInt64, y UInt64, t UInt64, k UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE s1 (x UInt64, y UInt64, j UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE s2 (j UInt64, t UInt64) ENGINE = MergeTree ORDER BY j SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE s3 (k UInt64, t UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO s0 SELECT number, number + 1, 1, number FROM numbers(10000);
INSERT INTO s1 SELECT number, number + 1, number FROM numbers(1000);
INSERT INTO s2 SELECT number, 1 FROM numbers(100);
INSERT INTO s3 SELECT number, 1 FROM numbers(1000);

SET query_plan_optimize_join_order_algorithm = 'greedy';
-- The other key is a condition of the join.
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM s0 JOIN s1 ON s0.x = s1.x OR s0.y = s1.y JOIN s2 ON s2.j = s1.j AND s2.t <=> s0.t JOIN s3 ON s3.k = s0.k
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
-- The other key comes from columns equal through another table.
SET enable_join_transitive_predicates = 1;
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM s0 JOIN s1 ON s0.x = s1.x OR s0.y = s1.y JOIN s2 ON s2.j = s1.j JOIN s3 ON s3.k = s0.k AND s3.t = s0.t AND s3.t = s2.t
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';

-- An OR is not a join key where the join does not run on its branches.
CREATE TABLE u0 (x UInt64, y UInt64, t UInt64, m UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE u1 (x UInt64, y UInt64, k UInt64, n UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE u2 (k UInt64, lo UInt64, hi UInt64, m UInt64, n UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO u0 SELECT number, number + 1, number, 1 FROM numbers(1000);
INSERT INTO u1 SELECT number, number + 1, number, 1 FROM numbers(2000);
INSERT INTO u2 SELECT number, number, number + 10, 1, 1 FROM numbers(100);

SET query_plan_optimize_join_order_algorithm = 'dpsize';
-- A join with two inequalities and no equality runs as an `ie_join` join, with the OR only as its filter.
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM u0 JOIN u1 ON u0.x = u1.x OR u0.y = u1.y JOIN u2 ON u2.k = u1.k AND u2.lo <= u0.t AND u2.hi >= u0.t
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
-- With `grace_hash`, a join on the branches of an OR is not supported.
SET query_plan_optimize_join_order_algorithm = 'greedy', join_algorithm = 'grace_hash';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM u0 JOIN u2 ON u2.m = u0.m JOIN u1 ON (u0.x = u1.x OR u0.y = u1.y) AND u2.n = u1.n
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';

-- A `Join` engine table is joined on its own key only, so an OR with another table is not a key there.
CREATE TABLE w0 (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE wj (k UInt64, v UInt64, w UInt64) ENGINE = Join(ALL, INNER, k);
CREATE TABLE w1 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO w0 SELECT number % 100, number FROM numbers(1000);
INSERT INTO wj SELECT number, number, number + 1 FROM numbers(100);
INSERT INTO w1 SELECT number, number + 1 FROM numbers(100);

SET query_plan_optimize_join_order_algorithm = 'greedy', join_algorithm = 'hash';
SELECT count() FROM w0 JOIN wj ON w0.k = wj.k JOIN w1 ON w1.x = wj.v OR w1.y = wj.w;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SELECT count() FROM w0 JOIN wj ON w0.k = wj.k JOIN w1 ON w1.x = wj.v OR w1.y = wj.w;

-- An OR of an inner join placed at an outer join step is a filter after that join, not its key.
CREATE TABLE v0 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE v1 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE v2 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO v0 SELECT number FROM numbers(10);
INSERT INTO v1 SELECT number, number + 1 FROM numbers(10000);
INSERT INTO v2 SELECT number, number + 1 FROM numbers(1000);

SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_convert_outer_join_to_inner_join = 0;
SELECT if(match(explain, '⋈|⟕|⟖'), replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN SELECT count() FROM v0 LEFT JOIN v1 ON 1 JOIN v2 ON v2.x = v1.x OR v2.y = v1.y
) WHERE match(explain, '⋈|⟕|⟖') OR explain LIKE '%Type: %';
SELECT count() FROM v0 LEFT JOIN v1 ON 1 JOIN v2 ON v2.x = v1.x OR v2.y = v1.y;

-- With `auto`, a join runs on the branches of an OR only where the OR is its only condition.
SET join_algorithm = 'auto';
SELECT if(explain LIKE '%⋈%', replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', ''), extract(explain, 'Type: [a-z]+')) FROM (
EXPLAIN WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) WHERE explain LIKE '%⋈%' OR explain LIKE '%Type: %';
SELECT count(), sum(cnt), sum(cityHash64(x, y)) FROM (
WITH a1 AS (SELECT c2, c1, c4 = 'a' AS c7 FROM t1 FINAL WHERE c2 IN (_CAST(['00000000-0000-0000-0000-000000000001'], 'Array(UUID)')) ORDER BY c3, c2, c1 LIMIT 0, 10000)
SELECT a2.c2, a2.c1, count() FROM a1 AS a2 INNER JOIN t2 AS a3 ON (a3.c1 = a2.c2) AND (a3.c5 IS NULL) AND (((a2.c7 = 1) AND (a3.c3 = a2.c1)) OR ((a2.c7 = 0) AND (a3.c2 = a2.c1))) INNER JOIN t1 AS a4 FINAL ON (a4.c2 = a3.c1) AND (a4.c1 = if(a2.c7 = 1, a3.c2, a3.c3)) GROUP BY 1, 2
) AS s (x, y, cnt);

CREATE TABLE ga (x UInt64, y UInt64, a UInt64, v UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE gb (x UInt64, y UInt64, w UInt64, d UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE gc (a UInt64, z UInt64, c UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE gd (c UInt64, d UInt64) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8192, auto_statistics_types = 'basic, uniq_v2';
INSERT INTO ga SELECT number * 100, number * 100 + 1, number, number FROM numbers(100);
INSERT INTO gb SELECT number, number + 1, number, number FROM numbers(10000);
INSERT INTO gc SELECT number, 101 * number, number FROM numbers(100);
INSERT INTO gd SELECT intDiv(number, 100), number FROM numbers(10000);

-- Joining `gb` to `ga ⋈ gc` puts the OR and `gc.z = ga.v + gb.w` into one join, which has no key.
SELECT count() FROM ga JOIN gb ON ga.x = gb.x OR ga.y = gb.y JOIN gc ON gc.a = ga.a AND gc.z = ga.v + gb.w JOIN gd ON gd.c = gc.c AND gd.d = gb.d;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE ta;
DROP TABLE tb;
DROP TABLE tc;
DROP TABLE ra;
DROP TABLE rb;
DROP TABLE rc;
DROP TABLE s0;
DROP TABLE s1;
DROP TABLE s2;
DROP TABLE s3;
DROP TABLE u0;
DROP TABLE u1;
DROP TABLE u2;
DROP TABLE w0;
DROP TABLE wj;
DROP TABLE w1;
DROP TABLE v0;
DROP TABLE v1;
DROP TABLE v2;
DROP TABLE ga;
DROP TABLE gb;
DROP TABLE gc;
DROP TABLE gd;
