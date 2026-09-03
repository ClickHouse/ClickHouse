-- Tags: no-fasttest

DROP TABLE IF EXISTS test_collate;

CREATE TABLE test_collate (x UInt32, s Nullable(String)) ENGINE=Memory();

INSERT INTO test_collate VALUES (1, 'Ё'), (1, 'ё'), (1, 'а'), (1, null), (2, 'А'), (2, 'я'), (2, 'Я'), (2, null);

SELECT 'Order by without collate';
SELECT * FROM test_collate ORDER BY s, x;
SELECT 'Order by with collate';
SELECT * FROM test_collate ORDER BY s COLLATE 'ru', x;

SELECT 'Order by tuple without collate';
SELECT * FROM test_collate ORDER BY x, s;
SELECT 'Order by tuple with collate';
SELECT * FROM test_collate ORDER BY x, s COLLATE 'ru';

DROP TABLE test_collate;

