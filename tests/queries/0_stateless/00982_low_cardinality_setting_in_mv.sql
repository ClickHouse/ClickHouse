DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
DROP TABLE IF EXISTS mat_view;

CREATE TABLE test1 (a LowCardinality(String)) ENGINE=MergeTree() ORDER BY a;
CREATE TABLE test2 (a UInt64) engine=MergeTree() ORDER BY a;
CREATE MATERIALIZED VIEW test_mv TO test2 AS SELECT toUInt64(a = 'test') FROM test1;

DROP TABLE test_mv;
DROP TABLE test1;
DROP TABLE test2;
