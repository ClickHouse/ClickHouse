-- Tags: use-rocksdb


DROP TABLE IF EXISTS rdb_str_pk;
DROP TABLE IF EXISTS t_null;

CREATE TABLE rdb_str_pk (key String, value String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb_str_pk VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

CREATE TABLE t_null (k Nullable(String)) ENGINE = TinyLog;
INSERT INTO t_null VALUES ('a'), ('b'), ('c'), ('d'), (NULL);

SELECT 'new planner inner';
SELECT key, value FROM (SELECT k AS key FROM t_null) AS t
INNER JOIN rdb_str_pk USING (key) ORDER BY key;

SELECT 'new planner left';
SELECT key, value FROM (SELECT k AS key FROM t_null) AS t
LEFT JOIN rdb_str_pk USING (key) ORDER BY key NULLS LAST, value;

-- Confirm DirectKeyValueJoin is still picked (mismatch would imply a regression
-- where `tryDirectJoin` declined to handle the Nullable left key).
SELECT 'explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_null) AS t
    INNER JOIN rdb_str_pk USING (key)
) WHERE explain LIKE '%Algorithm:%';

DROP TABLE rdb_str_pk;
DROP TABLE t_null;
