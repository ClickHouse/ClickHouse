DROP TABLE IF EXISTS test1;
CREATE TABLE test1 (a Int, b Int, c int,d int) ENGINE = MergeTree() PRIMARY KEY a;
insert into test1 values(1,1,2,2);
select case a+b when c then 'c' when d then 'd' end from test1;
DROP TABLE test1;
