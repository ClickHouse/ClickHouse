drop table if exists tab;
create table tab (x LowCardinality(String)) engine = MergeTree order by tuple();

insert into tab values ('a'), ('bb'), ('a'), ('cc');

select count() as c, x in ('a', 'bb') as g from tab group by g order by c;

drop table if exists tab;

-- https://github.com/ClickHouse/ClickHouse/issues/44503
CREATE TABLE test(key Int32) ENGINE = MergeTree ORDER BY (key) SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into test select intDiv(number,100) from numbers(10000000);
SELECT COUNT() FROM test WHERE key <= 100000 AND (NOT (toLowCardinality('') IN (SELECT '')));
