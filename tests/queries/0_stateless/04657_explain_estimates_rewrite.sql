-- `EXPLAIN ESTIMATE` must report the part, row and mark counts that `system.parts` reports for the
-- same table, with the final mark of the part excluded.

DROP TABLE IF EXISTS test;

CREATE TABLE test (i Int64) ENGINE = MergeTree() ORDER BY i SETTINGS index_granularity = 16;
INSERT INTO test SELECT number FROM numbers(128);
OPTIMIZE TABLE test;

-- sum(marks) - 1 because EXPLAIN ESTIMATE does not include the final mark of the part.
SELECT any(database), any(table), count() AS parts, sum(rows) AS rows, sum(marks) - 1 AS marks FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active = 1 GROUP BY (database, table);
EXPLAIN ESTIMATE SELECT * FROM test;

DROP TABLE test;
