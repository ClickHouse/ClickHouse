DROP TABLE IF EXISTS test_02559;

CREATE TABLE test_02559 (id UInt64) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_02559 SELECT number FROM numbers(10);

SET enable_multiple_prewhere_read_steps=true, move_all_conditions_to_prewhere=true;

-- { echoOn }

SELECT cast(id as UInt16) AS id16 FROM test_02559 PREWHERE id16 and (id % 40000) LIMIT 10;

SELECT cast(id as UInt16) AS cond1, (id % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond LIMIT 10;

SELECT cast(id as UInt16) AS cond1, (id % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond AND id > 4 LIMIT 10;

SELECT cast(id as UInt16) AS cond1, (id % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE id > 5 AND cond LIMIT 10;

SELECT cast(id as UInt16) AS cond1, (id % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond1 AND id > 6 AND cond2 LIMIT 10;

SELECT cast(id as UInt16) AS cond1 FROM test_02559 PREWHERE cond1 LIMIT 10; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SELECT count() FROM test_02559 PREWHERE 1 OR ignore(id) WHERE ignore(id)=0;

-- { echoOff }

DROP TABLE test_02559;
