SET enable_analyzer=1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select=2;
SET enable_global_with_statement=1;

DROP TABLE IF EXISTS test_insert SYNC;

CREATE TABLE test_insert (c1 String, c2 UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03632/tables/test_insert', '{replica}')
ORDER BY ();

INSERT INTO test_insert
WITH cte_test AS (SELECT '1234', 1)
SELECT * FROM cte_test;

SELECT count() FROM test_insert;

DROP TABLE test_insert;
