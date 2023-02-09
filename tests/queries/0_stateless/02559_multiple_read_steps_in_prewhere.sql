DROP TABLE IF EXISTS test_02559;

CREATE TABLE test_02559 (id UInt64) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_02559 SELECT number FROM numbers(10);

SET enable_multiple_prewhere_read_steps=true, move_all_conditions_to_prewhere=true;

-- { echoOn }

SELECT cast(id as UInt16) AS id16 FROM test_02559 PREWHERE id16 and (id % 40000) LIMIT 10;

SELECT cast(id as UInt16) AS id16, (id % 40000) AS id40000, (id16 AND id40000) AS cond FROM test_02559 PREWHERE cond LIMIT 10;

SELECT cast(id as UInt16) AS id16 FROM test_02559 PREWHERE id16 LIMIT 10;

SELECT count() FROM test_02559 PREWHERE 1 OR ignore(id) WHERE ignore(id)=0;

-- { echoOff }

DROP TABLE test_02559;
