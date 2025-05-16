DROP TABLE IF EXISTS test_02559;

CREATE TABLE test_02559 (id1 UInt64, id2 UInt64) ENGINE=MergeTree ORDER BY id1 SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO test_02559 SELECT number, number FROM numbers(10);

DROP ROW POLICY IF EXISTS 02559_filter_1 ON test_02559;
DROP ROW POLICY IF EXISTS 02559_filter_2 ON test_02559;

SET enable_multiple_prewhere_read_steps=true, move_all_conditions_to_prewhere=true;

-- { echoOn }

SELECT cast(id1 as UInt16) AS id16 FROM test_02559 PREWHERE id16 and (id2 % 40000) LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1, (id2 % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1, (if(id2 > 3, id2, NULL) % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1, (id2 % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond AND id2 > 4 LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1, (id2 % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE id2 > 5 AND cond LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1, (id2 % 40000) AS cond2, (cond1 AND cond2) AS cond FROM test_02559 PREWHERE cond1 AND id2 > 6 AND cond2 LIMIT 10;

SELECT cast(id1 as UInt16) AS cond1 FROM test_02559 PREWHERE cond1 LIMIT 10; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

SELECT * FROM test_02559 PREWHERE id1 <= 3 AND id2 > 0 WHERE (id1 + id2 < 15) LIMIT 10;

SELECT count() FROM test_02559 PREWHERE id2>=0 AND (1 OR ignore(id1)) WHERE ignore(id1)=0;

SELECT count() FROM test_02559 PREWHERE ignore(id1);

SELECT count() FROM test_02559 PREWHERE 1 OR ignore(id1);

SELECT count() FROM test_02559 PREWHERE ignore(id1) AND id2 > 0;

SELECT count() FROM test_02559 PREWHERE (1 OR ignore(id1)) AND id2 > 0;

SELECT count() FROM test_02559 PREWHERE (id1 <= 10 AND id2 > 0) AND ignore(id1);

SELECT count() FROM test_02559 PREWHERE ignore(id1) AND (id1 <= 10 AND id2 > 0);

SELECT count() FROM test_02559 PREWHERE (id1 <= 10 AND id2 > 0) AND (1 OR ignore(id1));

SELECT count() FROM test_02559 PREWHERE (1 OR ignore(id1)) AND (id1 <= 10 AND id2 > 0);

CREATE ROW POLICY 02559_filter_1 ON test_02559 USING id2=2 AS permissive TO ALL;
SELECT * FROM test_02559;

CREATE ROW POLICY 02559_filter_2 ON test_02559 USING id2<=2 AS restrictive TO ALL;
SELECT * FROM test_02559;

-- { echoOff }

DROP ROW POLICY IF EXISTS 02559_filter_1 ON test_02559;
DROP ROW POLICY IF EXISTS 02559_filter_2 ON test_02559;

DROP TABLE test_02559;
