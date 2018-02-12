DROP TABLE IF EXISTS test.nums;
DROP TABLE IF EXISTS test.nums_buf;

SET insert_allow_materialized_columns = 1;

CREATE TABLE test.nums ( n UInt64, m UInt64 MATERIALIZED n+1 ) ENGINE = Log;
CREATE TABLE test.nums_buf AS test.nums ENGINE = Buffer(test, nums, 1, 10, 100, 1, 3, 10000000, 100000000);

INSERT INTO test.nums_buf (n) VALUES (1);
INSERT INTO test.nums_buf (n) VALUES (2);
INSERT INTO test.nums_buf (n) VALUES (3);
INSERT INTO test.nums_buf (n) VALUES (4);
INSERT INTO test.nums_buf (n) VALUES (5);

SELECT n,m FROM test.nums ORDER BY n;
SELECT n,m FROM test.nums_buf ORDER BY n;

DROP TABLE IF EXISTS test.nums;
DROP TABLE IF EXISTS test.nums_buf;
