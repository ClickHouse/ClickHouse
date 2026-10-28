-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- no-fasttest: rocksdb is not enabled in fasttest.

-- A tuple-equality lookup on a composite key, with a `LowCardinality` element in the tuple constant.

DROP TABLE IF EXISTS t_kv_lc_tuple;
DROP TABLE IF EXISTS t_dim_lc;

CREATE TABLE t_kv_lc_tuple (k String, id UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY (k, id);
INSERT INTO t_kv_lc_tuple VALUES ('a', 1, 'x'), ('b', 2, 'y');

CREATE TABLE t_dim_lc (name LowCardinality(String), id UInt64) ENGINE = Memory;
INSERT INTO t_dim_lc VALUES ('a', 1);

-- Nullable(Tuple(LowCardinality(String), UInt64)) from a scalar subquery.
SELECT v FROM t_kv_lc_tuple WHERE (k, id) = (SELECT (name, id) FROM t_dim_lc LIMIT 1);
SELECT count() FROM (EXPLAIN actions = 1 SELECT v FROM t_kv_lc_tuple WHERE (k, id) = (SELECT (name, id) FROM t_dim_lc LIMIT 1)) WHERE explain ILIKE '%ReadType: GetKeys%';

DROP TABLE t_kv_lc_tuple;
DROP TABLE t_dim_lc;
