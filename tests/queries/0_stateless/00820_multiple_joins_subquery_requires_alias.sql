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

SET joined_subquery_requires_alias = 1;

select t1.a, t2.b, t3.c from table1 as t1 join table2 as t2 on t1.a = t2.a join table3 as t3 on t2.b = t3.b ORDER BY t1.a;
select t1.a, t2.b, t5.c from table1 as t1 join table2 as t2 on t1.a = t2.a join table5 as t5 on t1.a = t5.a AND t2.b = t5.b ORDER BY t1.a;

select t1.a, t2.a, t2.b, t3.b, t3.c, t5.a, t5.b, t5.c
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
join table5 as t5 on t3.c = t5.c
ORDER BY t1.a
FORMAT PrettyCompactNoEscapes;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on t1_a = t2_a
join table3 as t3 on t2_b = t3_b
ORDER BY t1.a;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
ORDER BY t1.a;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b
ORDER BY t1.a;

select t1.a, t2.a, t2.b, t3.b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b
ORDER BY t1.a;

select t1.a, t2.a, t2.b, t3.b
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
ORDER BY t1.a;

select table1.a, table2.a, table2.b, table3.b
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b
ORDER BY t1.a;

select t1.*, t2.*, t3.*
from table1 as t1
join table2 as t2 on table1.a = table2.a
join table3 as t3 on table2.b = table3.b
ORDER BY t1.a
FORMAT PrettyCompactNoEscapes;

select *
from table1 as t1
join table2 as t2 on t1.a = t2.a
join table3 as t3 on t2.b = t3.b
ORDER BY t1.a
FORMAT PrettyCompactNoEscapes;

select t1.a as t1_a, t2.a as t2_a, t2.b as t2_b, t3.b as t3_b,
    (t1.a + table2.b) as t1_t2_x, (table1.a + table3.b) as t1_t3_x, (t2.b + t3.b) as t2_t3_x
from table1 as t1
join table2 as t2 on t1_a = t2_a
join table3 as t3 on t2_b = t3_b
ORDER BY t1.a;

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;
DROP TABLE table5;
