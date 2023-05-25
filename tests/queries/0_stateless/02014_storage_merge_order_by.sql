DROP TABLE IF EXISTS short;
DROP TABLE IF EXISTS long;
DROP TABLE IF EXISTS merged;

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

DROP TABLE IF EXISTS short;
DROP TABLE IF EXISTS long;
DROP TABLE IF EXISTS merged;
