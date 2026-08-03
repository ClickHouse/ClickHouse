-- Tags: shard

CREATE TABLE t_neg (A Int64) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_neg SELECT -number FROM numbers(1000);
CREATE TABLE dist_neg AS t_neg ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg);
CREATE TABLE dist_one AS t_neg ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_neg);

-- A sibling whose type MATCHES the declared type, so the outer table sees no mismatch of its
-- own and the nested and aliased carriers below are genuinely exercised.
CREATE TABLE t_pos (A UInt64) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_pos SELECT number + 100000 FROM numbers(100);
CREATE TABLE dist_pos AS t_pos ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_pos);

CREATE TABLE m_retyped (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_one     (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_one$');
CREATE TABLE m_mixed   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^(dist_neg|dist_pos)$');
CREATE TABLE m_inner   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_outer   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^m_inner$');
CREATE TABLE a_inner ENGINE = Alias(currentDatabase(), 'm_inner');
CREATE TABLE m_alias   (`A` UInt64) ENGINE = Merge(currentDatabase(), '^(a_inner|dist_pos)$');
CREATE TABLE m_ok      (`A` Int64)  ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_ok_u    (`A` UInt64) ENGINE = Merge(currentDatabase(), '^dist_pos$');

-- DISTINCT drops the per-shard duplication, so the expected sequence does not depend on how
-- many streams the pipeline happens to use. The sort stays at the top level: a subquery ORDER BY
-- is not preserved by its parent, which makes any nested form report false failures.
SELECT '-- smallest and largest values through a top-level ORDER BY';
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_retyped ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_one     ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_mixed   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_mixed   ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_outer   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_alias   ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_alias   ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM m_ok      ORDER BY A ASC  LIMIT 3;
SELECT DISTINCT A FROM m_ok_u    ORDER BY A ASC  LIMIT 3;

SELECT '-- the reported DISTINCT query, both analyzers';
SELECT DISTINCT plus(0, A) AS v FROM m_retyped ORDER BY ALL ASC LIMIT 3 SETTINGS enable_analyzer = 1;
SELECT DISTINCT plus(0, A) AS v FROM m_retyped ORDER BY ALL ASC LIMIT 3 SETTINGS enable_analyzer = 0;
SELECT count(), min(v), max(v) FROM (SELECT DISTINCT plus(0, A) AS v FROM m_retyped);

SELECT '-- aggregate states must survive the child stage as well';
SELECT min(A), max(A) FROM m_retyped;
SELECT min(A), max(A) FROM m_outer;
SELECT min(A), max(A) FROM m_alias;
SELECT min(A), max(A) FROM m_ok;

SELECT '-- window sort with no top-level ORDER BY (the shape the fuzzer reported)';
SELECT max(s) FROM (SELECT sum(toInt64(A)) OVER (ORDER BY A ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM m_one);

SELECT '-- LIMIT BY and the negative LIMIT sibling';
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC LIMIT 1 BY A LIMIT 3;
SELECT DISTINCT A FROM m_retyped ORDER BY A ASC LIMIT -3;

SELECT '-- UNION ALL forwards the claim upward';
SELECT count(), uniqExact(A) FROM (SELECT A FROM m_retyped UNION ALL SELECT A FROM m_ok_u);

SELECT '-- GROUP BY over the nested and aliased carriers';
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_outer GROUP BY A);
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_alias GROUP BY A);

SELECT '-- controls: matching types keep the stage pushed down, so the merge step survives';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_ok ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT plus(0, A) FROM m_ok_u ORDER BY ALL ASC) WHERE explain ILIKE '%Merge sorted streams%';

SELECT '-- a self-referential Merge must not hang';
CREATE TABLE m_self (`A` UInt64) ENGINE = Merge(currentDatabase(), '^m_self$');
SELECT count() FROM m_self;

-- Read-in-order rejects these casts through its own monotonicity analysis, so this case is
-- already correct before the fix. It guards that conclusion, it does not witness the fix.
SELECT '-- MergeTree child under read-in-order and aggregation-in-order';
CREATE TABLE m_mt (`A` UInt64) ENGINE = Merge(currentDatabase(), '^t_neg$');
SELECT DISTINCT A FROM m_mt ORDER BY A ASC LIMIT 3 SETTINGS optimize_read_in_order = 1;
SELECT count(), min(A), max(A) FROM (SELECT A, count() FROM m_mt GROUP BY A SETTINGS optimize_aggregation_in_order = 1);
