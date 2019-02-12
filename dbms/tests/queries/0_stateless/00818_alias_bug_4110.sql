select s.a as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as a, s.a as b from (select 10 as a) s;
select s.a + 1 as a, s.a + 1 as b from (select 10 as a) s;
select s.a + 1 as b, s.a + 2 as a from (select 10 as a) s;
select s.a + 2 as b, s.a + 1 as a from (select 10 as a) s;

SELECT 0 as t FROM (SELECT 1 as t) as inn WHERE inn.t = 1;
SELECT sum(value) as value FROM (SELECT 1 as value) as data WHERE data.value > 0;

USE test;
DROP TABLE IF EXISTS test;
CREATE TABLE test (field String, not_field String) ENGINE = Memory;
INSERT INTO test (field, not_field) VALUES ('123', '456')
SELECT test.field AS other_field, test.not_field AS field FROM test;
DROP TABLE test;
