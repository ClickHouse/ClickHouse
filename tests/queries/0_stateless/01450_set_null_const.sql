DROP TABLE IF EXISTS test_mtree;

CREATE TABLE test_mtree (`x` String, INDEX idx x TYPE set(10) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_mtree VALUES ('Hello, world');
SELECT count() FROM test_mtree WHERE x = NULL;

DROP TABLE test_mtree;
