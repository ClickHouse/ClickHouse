DROP TABLE IF EXISTS collate_test1;
DROP TABLE IF EXISTS collate_test2;
DROP TABLE IF EXISTS collate_test3;

CREATE TABLE collate_test1 (x UInt32, s Array(String)) ENGINE=Memory();
CREATE TABLE collate_test2 (x UInt32, s Array(LowCardinality(Nullable(String)))) ENGINE=Memory();
CREATE TABLE collate_test3 (x UInt32, s Array(Array(String))) ENGINE=Memory();

INSERT INTO collate_test1 VALUES (1, ['Ё']), (1, ['ё']), (1, ['а']), (2, ['А']), (2, ['я', 'а']), (2, ['Я']), (1, ['ё','а']), (1, ['ё', 'я']), (2, ['ё', 'а', 'а']);
INSERT INTO collate_test2 VALUES (1, ['Ё']), (1, ['ё']), (1, ['а']), (2, ['А']), (2, ['я']), (2, [null, 'Я']), (1, ['ё','а']), (1, ['ё', null, 'я']), (2, ['ё', 'а', 'а', null]);
INSERT INTO collate_test3 VALUES (1, [['а', 'я'], ['а', 'ё']]), (1, [['а', 'Ё'], ['ё', 'я']]), (2, [['ё']]), (2, [['а', 'а'], ['я', 'ё']]);

SELECT * FROM collate_test1 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test1 ORDER BY x, s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test2 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test2 ORDER BY x, s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test3 ORDER BY s COLLATE 'ru';
SELECT '';

SELECT * FROM collate_test3 ORDER BY x, s COLLATE 'ru';
SELECT '';

DROP TABLE collate_test1;
DROP TABLE collate_test2;
DROP TABLE collate_test3;

