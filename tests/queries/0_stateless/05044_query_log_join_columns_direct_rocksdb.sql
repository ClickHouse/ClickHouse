-- Tags: no-old-analyzer, use-rocksdb, no-fasttest
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the rocksdb engine is not enabled by default.

-- The `direct` join algorithm needs a key-value storage on the right side. A dictionary is the one
-- covered by the other tests of these columns; `EmbeddedRocksDB` is the other kind of storage that
-- offers the same key-value interface, and a join over it reports `DIRECT` as well. It lives in a test
-- of its own because the rocksdb engine is not built in every configuration.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides
-- swapped, which reverses LEFT and RIGHT. Disable that so the kinds are the ones the queries are
-- written with.
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS kv;

CREATE TABLE t1 (a UInt64) ENGINE = Memory;
CREATE TABLE kv (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO kv SELECT number, toString(number) FROM numbers(10);

SELECT 'direct join with a rocksdb table';
SELECT count() FROM t1 JOIN kv ON t1.a = kv.key
FORMAT Null
SETTINGS log_comment = '05044_join_rocksdb_a_inner', join_algorithm = 'direct';
SELECT count() FROM t1 LEFT JOIN kv ON t1.a = kv.key
FORMAT Null
SETTINGS log_comment = '05044_join_rocksdb_b_left', join_algorithm = 'direct';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05044\_join\_rocksdb\_%'
ORDER BY log_comment;

DROP TABLE kv;
DROP TABLE t1;
