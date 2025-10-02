create table p3 (x Int64, y Int64) engine MergeTree order by (x, y);
insert into p3 values (100, 100);
explain indexes=1,projections=1 select * from p3 where pointInPolygon((x, y), [(0,0),(0,150),(150,150),(150,0)]) format Null;
