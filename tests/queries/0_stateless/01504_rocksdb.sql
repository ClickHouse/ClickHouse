DROP TABLE IF EXISTS test;
create table test (key String, value UInt32) Engine=EmbeddedRocksdb;

insert into test select '1_1', number from numbers(10000);
select count(1) from test;

insert into test select concat(toString(number), '_1'), number from numbers(10000);
select sum(value) from test where key in ('1_1', '99_1', '900_1');


DROP TABLE IF EXISTS test;
create table test (key String, value  UInt64) Engine=EmbeddedRocksdb;

insert into test select toString(number%3) as key, sum(number) as value from numbers(1000000) group by key;

select key, sum(value) from test group by key order by key;

truncate table test;
select count(1) from test;

DROP TABLE IF EXISTS test;

