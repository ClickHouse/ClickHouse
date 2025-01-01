SELECT 'Issue 68605';
DROP TABLE IF EXISTS test_33602;
CREATE TABLE test_33602 (name String, score UInt8, user_level  Enum8('LOW' = 1, 'MEDIUM' = 2, 'HIGH' = 3)) ENGINE=Memory;
SELECT any(user_level) FROM test_33602;
SELECT any(user_level), any(name), any(score) FROM test_33602;
SELECT anyLast(user_level), anyLast(name), anyLast(score) FROM test_33602;

SELECT 'Empty Enum8 table:';
CREATE TABLE test_33602_t0a (e Enum8('LOW' = 123, 'MEDIUM' = 12, 'HIGH' = 33)) ENGINE=Memory;
SELECT any(e) FROM test_33602_t0a;
SELECT anyLast(e) FROM test_33602_t0a;

SELECT 'Enum8 table with HIGH value:';
INSERT INTO test_33602_t0a VALUES('HIGH');
SELECT any(e) FROM test_33602_t0a;
SELECT anyLast(e) FROM test_33602_t0a;


SELECT 'Empty Enum16 table:';
CREATE TABLE test_33602_t0b (e Enum16('LOW' = 123, 'MEDIUM' = 12, 'HIGH' = 33)) ENGINE=Memory;
SELECT any(e) FROM test_33602_t0b;
SELECT anyLast(e) FROM test_33602_t0b;

SELECT 'Enum16 table with HIGH value:';
INSERT INTO test_33602_t0b VALUES('HIGH');
SELECT any(e) FROM test_33602_t0a;
SELECT anyLast(e) FROM test_33602_t0a;
