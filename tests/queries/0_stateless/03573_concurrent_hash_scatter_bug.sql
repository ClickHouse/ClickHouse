DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS test_table_join_1;
DROP TABLE IF EXISTS test_table_join_2;

CREATE TABLE test_table_join_1 ( id UInt8, value String ) ENGINE = TinyLog;
CREATE TABLE test_table_join_2 ( id UInt16, value String ) ENGINE = TinyLog;
INSERT INTO test_table_join_1 VALUES (0,'Join_1_Value_0'),(1,'Join_1_Value_1'),(2,'Join_1_Value_2'),(0,'Join_1_Value_0'),(1,'Join_1_Value_1'),(2,'Join_1_Value_2');
INSERT INTO test_table_join_2 VALUES (0,'Join_2_Value_0'),(1,'Join_2_Value_1'),(3,'Join_2_Value_3'),(0,'Join_2_Value_0'),(1,'Join_2_Value_1'),(3,'Join_2_Value_3');

CREATE TABLE t0 (i Int64, j Int16) ENGINE = MergeTree ORDER BY i;
INSERT INTO t0 VALUES (1, 1), (2, 2);

SET join_algorithm = 'parallel_hash';

SELECT i FROM t0 ANY JOIN (SELECT 3 AS k) AS x ON x.k = j;

INSERT INTO t0 SELECT number, number FROM numbers_mt(100_000) WHERE number != 3;
SELECT i FROM t0 ANY JOIN (SELECT 3 AS k) AS x ON x.k = j;

SELECT *
FROM test_table_join_1 AS t1
ANY INNER JOIN test_table_join_2 AS t2 USING (id)
ORDER BY id ASC, t1.value ASC;
