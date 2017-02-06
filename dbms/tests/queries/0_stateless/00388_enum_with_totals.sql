DROP TABLE IF EXISTS test.enum_totals;
CREATE TABLE test.enum_totals (e Enum8('hello' = 1, 'world' = 2)) ENGINE = Memory;
INSERT INTO test.enum_totals VALUES ('hello'), ('world'), ('world');

SELECT e, count() FROM test.enum_totals GROUP BY e WITH TOTALS ORDER BY e;
DROP TABLE test.enum_totals;
