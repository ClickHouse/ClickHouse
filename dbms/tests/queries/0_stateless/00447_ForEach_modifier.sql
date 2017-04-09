CREATE TABLE IF NOT EXISTS test.data (sketch Array(Int8)) ENGINE=Memory;

INSERT INTO test.data VALUES ([-1,-1,-1]), ([4,-1,2]), ([0,25,-1]), ([-1,-1,7]), ([-1,-1,-1]);

SELECT max(sketch) FROM test.data;

SELECT maxArray(sketch) FROM test.data;

SELECT maxForEach(sketch) FROM test.data;

DROP TABLE test.data;
