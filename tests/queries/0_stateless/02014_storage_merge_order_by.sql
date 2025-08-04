DROP TABLE IF EXISTS short;
DROP TABLE IF EXISTS long;
DROP TABLE IF EXISTS merged;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;

CREATE TABLE short (e Int64, t DateTime ) ENGINE = MergeTree PARTITION BY e ORDER BY t;
CREATE TABLE long (e Int64, t DateTime ) ENGINE = MergeTree PARTITION BY (e, toStartOfMonth(t)) ORDER BY t;

insert into short select number % 11, toDateTime('2021-01-01 00:00:00') + number from numbers(1000);
insert into long select number % 11, toDateTime('2021-01-01 00:00:00') + number from numbers(1000);

CREATE TABLE merged as short ENGINE = Merge(currentDatabase(), 'short|long');

select sum(e) from (select * from merged order by t limit 10) SETTINGS optimize_read_in_order = 0;

select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 1;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 3;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 10;
select sum(e) from (select * from merged order by t limit 10) SETTINGS max_threads = 50;

CREATE TABLE t1 (key Int) ENGINE=MergeTree() ORDER BY key;
CREATE TABLE t2 (key Int) ENGINE=MergeTree() ORDER BY key;
CREATE TABLE d1 ENGINE=Distributed('test_shard_localhost', currentDatabase(), t2, rand());
CREATE TABLE m1 ENGINE=Merge(currentDatabase(), '^(t1|d1)$');
CREATE TABLE m2 ENGINE=Merge(currentDatabase(), '^(t1|m1)$');
INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);

SELECT * FROM m2 ORDER BY key ASC SETTINGS max_threads = 1;

DROP TABLE IF EXISTS short;
DROP TABLE IF EXISTS long;
DROP TABLE IF EXISTS merged;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
