DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 (a UInt32, b UInt32) ENGINE = Memory;
CREATE TABLE table2 (a UInt32, b UInt32) ENGINE = Memory;

INSERT INTO table1 SELECT number, number FROM numbers(10);
INSERT INTO table2 SELECT number * 2, number * 20 FROM numbers(6);

select t1.a t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = table2.a and t1_a = table2.a;

select t1.a t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = t2.a and t1.a = t2.a and t1_a = t2.a;

select t1.a as t1_a, t2.a t2_a
from table1 as t1
join table2 as t2 on table1.a = t2_a and t1.a = t2_a and t1_a = t2_a;

select t1.a t1_a, t2.a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = t2.a and t1_a = t2.a;

select t1.a t1_a, t2.a as t2_a
from table1 as t1
join table2 as t2 on table1.a = table2.a and t1.a = t2.a and t1_a = t2_a;

select *
from table1 as t1
join table2 as t2 on t1_a = t2_a
where (table1.a as t1_a) > 4 and (table2.a as t2_a) > 2;

select t1.*, t2.*
from table1 as t1
join table2 as t2 on t1_a = t2_a
where (t1.a as t1_a) > 2 and (t2.a as t2_a) > 4;

DROP TABLE table1;
DROP TABLE table2;
