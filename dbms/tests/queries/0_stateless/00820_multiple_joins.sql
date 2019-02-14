USE test;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;
DROP TABLE IF EXISTS table5;

CREATE TABLE table1 (a UInt32) ENGINE = Memory;
CREATE TABLE table2 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE table3 (b UInt32, c UInt32) ENGINE = Memory;
CREATE TABLE table5 (a UInt32, b UInt32, c UInt32) ENGINE = Memory;

INSERT INTO table1 SELECT number FROM numbers(21);
INSERT INTO table2 SELECT number * 2, number * 20 FROM numbers(11);
INSERT INTO table3 SELECT number * 30, number * 300 FROM numbers(10);
INSERT INTO table5 SELECT number * 5, number * 50, number * 500 FROM numbers(10);

SET allow_experimental_multiple_joins_emulation = 1;

SELECT 1 LIMIT 0;

-- FIXME: wrong names qualification
select a, b, c from table1 as t1 join table2 as t2 on t1.a = t2.a join table3 as t3 on b = t3.b;
select a, b, c from table1 as t1 join table2 as t2 on t1.a = t2.a join table5 as t5 on a = t5.a AND b = t5.b;
--select a, b, c from table1 as t1 join table2 as t2 on t1.a = t2.a join table3 as t3 on b = t3.b join table5 as t5 on c = t5.c;

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;
DROP TABLE table5;
