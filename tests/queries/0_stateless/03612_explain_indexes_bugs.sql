-- Tags: no-parallel-replicas
-- no-parallel-replicas because the output of explain is different. 
set enable_analyzer = 1;

create table points (x Int64, y Int64) engine MergeTree order by (x, y);
insert into points values (100, 100);
explain indexes=1 select * from points where pointInPolygon((x, y), [(0,0),(0,150),(150,150),(150,0)]);
explain indexes=1 select * from points where plus(minus(x, 1), 10) < 10;
explain indexes=1 select * from points where (plus(minus(x, 1), 10), minus(plus(y, 2), 20)) in (10, 20);
explain indexes=1 select * from points where (plus(minus(x, 1), 10), minus(plus(x, 2), 20)) in (10, 20);

create table morton (x UInt64, y UInt64) engine MergeTree order by mortonEncode(x, y);
insert into morton values (100, 200);
explain indexes=1 select * from morton where x > 100;
explain indexes=1 select x+y from morton where x+1 = 101;
select x+y from morton where x+1 = 101;
