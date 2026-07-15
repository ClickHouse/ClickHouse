-- Tags: no-parallel

DROP TABLE IF EXISTS t_cache_sparse;
SYSTEM DROP QUERY CACHE;

CREATE TABLE t_cache_sparse (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;

SYSTEM STOP MERGES t_cache_sparse;

INSERT INTO t_cache_sparse SELECT number, number FROM numbers(10000);
INSERT INTO t_cache_sparse SELECT number, 0 FROM numbers(10000);

SET max_threads = 1;

SELECT v FROM t_cache_sparse SETTINGS use_query_cache = 1, max_threads = 1 FORMAT Null;
SELECT v FROM t_cache_sparse SETTINGS use_query_cache = 1, max_threads = 1 FORMAT Null;
SELECT count() FROM system.query_cache WHERE query LIKE 'SELECT v FROM t_cache_sparse%';

DROP TABLE t_cache_sparse;
