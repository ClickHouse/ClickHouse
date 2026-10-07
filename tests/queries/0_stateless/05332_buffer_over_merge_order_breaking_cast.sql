-- Tags: shard

-- A Buffer logs a warning on every read of a destination whose types differ.
SET send_logs_level = 'error';

CREATE TABLE t_neg (A Int64) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_neg SELECT -number FROM numbers(1000);
CREATE TABLE dist_neg AS t_neg ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg);
CREATE TABLE m_neg (A Int64) ENGINE = Merge(currentDatabase(), '^dist_neg$');
CREATE TABLE m_neg_nullable (A Nullable(Int64)) ENGINE = Merge(currentDatabase(), '^dist_neg$');

-- `Int64` to `UInt64` flips the sign bit, so it reorders the values.
CREATE TABLE buf_u64 (A UInt64) ENGINE = Buffer(currentDatabase(), 'm_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_n (A UInt64) ENGINE = Buffer(currentDatabase(), 'm_neg_nullable', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
-- Declared types that differ but keep the order: they must keep the delegated stage.
CREATE TABLE buf_nullable (A Nullable(Int64)) ENGINE = Buffer(currentDatabase(), 'm_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_i128 (A Int128) ENGINE = Buffer(currentDatabase(), 'm_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);

SELECT '-- ORDER BY in the Buffer type';
SELECT DISTINCT A FROM buf_u64 ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM buf_u64 ORDER BY A ASC LIMIT 3;
SELECT '-- the reported query shape';
SELECT DISTINCT A FROM buf_u64_n ORDER BY ALL DESC NULLS FIRST LIMIT 3;
SELECT '-- the same values cast on the shards';
SELECT DISTINCT toUInt64(A) AS B FROM dist_neg ORDER BY B DESC LIMIT 3;
SELECT '-- reductions';
SELECT count(), uniqExact(A), min(A), max(A) FROM buf_u64;
SELECT A, count() FROM buf_u64 GROUP BY A ORDER BY A DESC LIMIT 3;
SELECT A FROM buf_u64 ORDER BY A DESC LIMIT 1 BY A LIMIT 3;
SELECT '-- only the order-breaking conversion gives up the delegated stage';
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM buf_u64 ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM buf_nullable ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM buf_i128 ORDER BY A ASC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM buf_nullable ORDER BY A ASC LIMIT 3;
SELECT DISTINCT A FROM buf_i128 ORDER BY A DESC LIMIT 3;
SELECT '-- a Merge behind a wrapper that forwards the stage and the read';
CREATE TABLE a_neg ENGINE = Alias(currentDatabase(), m_neg);
CREATE TABLE m_neg_tf AS merge(currentDatabase(), '^dist_neg$');
CREATE TABLE buf_i64 (A Int64) ENGINE = Buffer(currentDatabase(), 'm_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE src_null (A Int64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_neg TO m_neg AS SELECT A FROM src_null;
CREATE TABLE buf_u64_alias (A UInt64) ENGINE = Buffer(currentDatabase(), 'a_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_tf (A UInt64) ENGINE = Buffer(currentDatabase(), 'm_neg_tf', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_buf (A UInt64) ENGINE = Buffer(currentDatabase(), 'buf_i64', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_mv (A UInt64) ENGINE = Buffer(currentDatabase(), 'mv_neg', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
SELECT DISTINCT A FROM buf_u64_alias ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM buf_u64_tf ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM buf_u64_buf ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM buf_u64_mv ORDER BY A DESC LIMIT 3;
SELECT '-- a materialized view that converts the target types itself';
CREATE MATERIALIZED VIEW mv_u64 TO m_neg (A UInt64) AS SELECT toUInt64(A) AS A FROM src_null;
CREATE TABLE buf_mv_u64 (A UInt64) ENGINE = Buffer(currentDatabase(), 'mv_u64', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
SELECT DISTINCT A FROM mv_u64 ORDER BY A DESC LIMIT 3;
SELECT DISTINCT A FROM buf_mv_u64 ORDER BY A DESC LIMIT 3;
