CREATE TABLE test_table_1 (s String) ENGINE = GenerateRandom(1, 100);

SELECT max(length(s)) > 1 AS has_long_strings FROM (SELECT s FROM test_table_1 LIMIT 1000);

DROP TABLE IF EXISTS test_table_1;

SELECT '-';

CREATE TABLE test_table_2 (s String) ENGINE = GenerateRandom(1, 100, 3);

SELECT max(length(s)) > 1 AS has_long_strings FROM (SELECT s FROM test_table_2 LIMIT 1000);

DROP TABLE IF EXISTS test_table_2;
