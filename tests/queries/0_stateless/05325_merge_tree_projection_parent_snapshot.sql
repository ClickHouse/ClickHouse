-- Random settings limits: enable_shared_storage_snapshot_in_query=(1, None)
-- A read of a table after `mergeTreeProjection` of the same table in one query
-- must use the table's own columns and primary key, not the projection's.

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS t;

CREATE TABLE test (a Int32, b Int32, PROJECTION p (SELECT a, b, _part_offset ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO test SELECT number, -1 - number FROM numbers(10000);

SELECT (SELECT count() FROM mergeTreeProjection(currentDatabase(), test, p)), (SELECT count() FROM test WHERE b < -5000);

CREATE TABLE t (a Int32, b Int32, c Int32 DEFAULT 999, PROJECTION p (SELECT a, b ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t (a, b) VALUES (1, 10), (2, 20);

SELECT a, b, c FROM (SELECT a, b, 0 AS c FROM mergeTreeProjection(currentDatabase(), t, p) UNION ALL SELECT a, b, c FROM t) ORDER BY a, c;

-- The same through `merge`, with the table written before `mergeTreeProjection`.
SELECT count(), sum(l.c) FROM merge(currentDatabase(), '^t$') AS l JOIN mergeTreeProjection(currentDatabase(), t, p) AS r ON l.a = r.a;

DROP TABLE test;
DROP TABLE t;
