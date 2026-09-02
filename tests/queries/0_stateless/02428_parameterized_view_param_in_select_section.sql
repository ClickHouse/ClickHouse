-- https://github.com/ClickHouse/ClickHouse/issues/56564

create table t(z String, ts DateTime) Engine=Memory as 
select '1', '2020-01-01 00:00:00';

CREATE VIEW v1 AS
SELECT z, 'test' = {m:String} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

CREATE VIEW v2 AS
SELECT z, {m:String} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

CREATE VIEW v3 AS
SELECT z, {m:String} 
FROM t;
  
select * from v1(m='test');
select * from v2(m='test');
select * from v3(m='test');

drop table t;
drop view v1;
drop view v2;
drop view v3;

create table t(z String, ts DateTime) Engine=MergeTree ORDER BY z as 
select '1', '2020-01-01 00:00:00';

CREATE VIEW v1 AS
SELECT z, 'test' = {m:String} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

CREATE VIEW v2 AS
SELECT z, {m:String} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

CREATE VIEW v3 AS
SELECT z, {m:String} 
FROM t;
  
select * from v1(m='test');
select * from v2(m='test');
select * from v3(m='test');

drop table t;
drop view v1;
drop view v2;
drop view v3;

