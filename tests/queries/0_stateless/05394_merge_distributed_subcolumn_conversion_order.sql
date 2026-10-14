-- Tags: shard

-- A name that is a column on one side and a subcolumn or a virtual column on the other. The child or the shards
-- resolve it as their own subcolumn or column and sort by its type, so a conversion to the declared type that
-- breaks the order is handled like one between two columns: `Merge` reads the child at `FetchColumns`, and
-- `Distributed` refuses the query.

CREATE TABLE t_tup (a Tuple(x Int64), k Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tup SELECT tuple(-number), number FROM numbers(1000);
CREATE TABLE t_dotted (`a.x` Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dotted SELECT -number FROM numbers(1000);
-- The physical `_part` shadows the virtual column of the same name.
CREATE TABLE t_part (x UInt8, `_part` UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_part SELECT 0, number + 1 FROM numbers(1000);
-- A dynamic path of a `JSON` column is not among the listed subcolumns, it is only resolved by name.
CREATE TABLE t_json (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json SELECT toJSONString(map('x', -number)) FROM numbers(1000);

SELECT '-- Merge';
CREATE TABLE dist_tup AS t_tup ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_tup);
CREATE TABLE m_dotted (`a.x` UInt64) ENGINE = Merge(currentDatabase(), '^dist_tup$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT `a.x` FROM m_dotted ORDER BY `a.x` DESC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT `a.x` FROM m_dotted ORDER BY `a.x` DESC LIMIT 3;

CREATE TABLE dist_dotted AS t_dotted ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_dotted);
CREATE TABLE m_tuple (a Tuple(x UInt64)) ENGINE = Merge(currentDatabase(), '^dist_dotted$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT a.x FROM m_tuple ORDER BY a.x DESC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT a.x FROM m_tuple ORDER BY a.x DESC LIMIT 3;

CREATE TABLE dist_part AS t_part ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_part);
CREATE TABLE m_part (x UInt8) ENGINE = Merge(currentDatabase(), '^dist_part$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT _part FROM m_part ORDER BY _part DESC) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT _part FROM m_part ORDER BY _part DESC LIMIT 3;

CREATE TABLE dist_json AS t_json ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_json);
CREATE TABLE m_json (`j.x` UInt64) ENGINE = Merge(currentDatabase(), '^dist_json$');
SELECT count() FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT `j.x` FROM m_json ORDER BY `j.x` DESC SETTINGS allow_suspicious_types_in_order_by = 1) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT `j.x` FROM m_json ORDER BY `j.x` DESC LIMIT 3 SETTINGS allow_suspicious_types_in_order_by = 1;

SELECT '-- Distributed';
CREATE TABLE d_dotted (`a.x` UInt64, k Int64) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_tup);
SELECT DISTINCT `a.x` FROM d_dotted ORDER BY `a.x` DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
-- A mismatched name that nothing sorts by is still carried along.
SELECT k, `a.x` FROM d_dotted ORDER BY k DESC LIMIT 2;

CREATE TABLE d_tuple (a Tuple(x UInt64)) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_dotted);
SELECT DISTINCT a.x FROM d_tuple ORDER BY a.x DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }

CREATE TABLE d_part (x UInt8) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_part);
SELECT DISTINCT _part FROM d_part ORDER BY _part DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }

CREATE TABLE d_json (`j.x` UInt64) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_json);
SELECT DISTINCT `j.x` FROM d_json ORDER BY `j.x` DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
