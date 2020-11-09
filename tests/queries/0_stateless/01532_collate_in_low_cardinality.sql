DROP TABLE IF EXISTS test_collate;
DROP TABLE IF EXISTS test_collate_null;

CREATE TABLE test_collate (x UInt32, s LowCardinality(String)) ENGINE=Memory();
CREATE TABLE test_collate_null (x UInt32, s LowCardinality(Nullable(String))) ENGINE=Memory();

INSERT INTO test_collate VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (2, 'А'), (2, 'я'), (2, 'Я');
INSERT INTO test_collate_null VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (2, 'А'), (2, 'я'), (2, 'Я'), (1, null), (2, null);


SELECT 'Order by without collate';
SELECT * FROM test_collate ORDER BY s;
SELECT 'Order by with collate';
SELECT * FROM test_collate ORDER BY s COLLATE 'ru';

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate ORDER BY x, s COLLATE 'ru';

SELECT 'Order by without collate';
SELECT * FROM test_collate_null ORDER BY s;
SELECT 'Order by with collate';
SELECT * FROM test_collate_null ORDER BY s COLLATE 'ru';

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate_null ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate_null ORDER BY x, s COLLATE 'ru';


DROP TABLE test_collate;
DROP TABLE test_collate_null;
