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
SELECT '-- a name that is a column on one side and a subcolumn on the other';
CREATE TABLE t_neg_tuple (a Tuple(x Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_neg_tuple SELECT tuple(-number) FROM numbers(1000);
CREATE TABLE dist_neg_tuple AS t_neg_tuple ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg_tuple);
CREATE TABLE m_neg_tuple AS t_neg_tuple ENGINE = Merge(currentDatabase(), '^dist_neg_tuple$');
CREATE TABLE t_neg_dotted (`a.x` Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_neg_dotted SELECT -number FROM numbers(1000);
CREATE TABLE dist_neg_dotted AS t_neg_dotted ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg_dotted);
CREATE TABLE m_neg_dotted AS t_neg_dotted ENGINE = Merge(currentDatabase(), '^dist_neg_dotted$');
CREATE TABLE t_neg_json (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_neg_json SELECT concat('{"x": ', toString(-number), '}') FROM numbers(1000);
CREATE TABLE dist_neg_json AS t_neg_json ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_neg_json);
CREATE TABLE m_neg_json AS t_neg_json ENGINE = Merge(currentDatabase(), '^dist_neg_json$');
CREATE TABLE buf_u64_dotted (`a.x` UInt64) ENGINE = Buffer(currentDatabase(), 'm_neg_tuple', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_tuple (a Tuple(x UInt64)) ENGINE = Buffer(currentDatabase(), 'm_neg_dotted', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE buf_u64_json_path (`j.x` UInt64) ENGINE = Buffer(currentDatabase(), 'm_neg_json', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
CREATE TABLE src_tuple_null (a Tuple(x Int64)) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_u64_dotted TO m_neg_tuple (`a.x` UInt64) AS SELECT a FROM src_tuple_null;
SELECT DISTINCT `a.x` FROM buf_u64_dotted ORDER BY `a.x` DESC LIMIT 3;
SELECT DISTINCT a.x FROM buf_u64_tuple ORDER BY a.x DESC LIMIT 3;
SELECT DISTINCT `j.x` FROM buf_u64_json_path ORDER BY `j.x` DESC LIMIT 3 SETTINGS allow_suspicious_types_in_order_by = 1;
SELECT DISTINCT `a.x` FROM mv_u64_dotted ORDER BY `a.x` DESC LIMIT 3;
SELECT '-- a physical column that shadows a virtual one';
CREATE TABLE t_virtual (x UInt8, `_table` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_virtual SELECT 0, number + 1 FROM numbers(1000);
CREATE TABLE dist_virtual AS t_virtual ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_virtual);
CREATE TABLE m_virtual AS t_virtual ENGINE = Merge(currentDatabase(), '^dist_virtual$');
CREATE TABLE buf_virtual (x UInt8) ENGINE = Buffer(currentDatabase(), 'm_virtual', 1, 100000, 100000, 1000000000, 1000000000, 1000000000000, 1000000000000);
SELECT DISTINCT _table FROM buf_virtual ORDER BY _table DESC LIMIT 3;
