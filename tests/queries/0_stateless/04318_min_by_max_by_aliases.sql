-- Tests that min_by / max_by work as aliases for argMin / argMax
-- (SQL compatibility with Presto / Spark / DuckDB)

DROP TABLE IF EXISTS test_min_by_max_by;
CREATE TABLE test_min_by_max_by (arg String, val Int64) ENGINE = Memory;
INSERT INTO test_min_by_max_by VALUES ('a', 1), ('b', 2), ('c', 3);

SELECT max_by(arg, val) FROM test_min_by_max_by;
SELECT argMax(arg, val) FROM test_min_by_max_by;
SELECT min_by(arg, val) FROM test_min_by_max_by;
SELECT argMin(arg, val) FROM test_min_by_max_by;

DROP TABLE test_min_by_max_by;
