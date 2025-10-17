-- https://github.com/ClickHouse/ClickHouse/issues/47432
SET enable_analyzer=1;
create table t1
engine = MergeTree()
order by tuple()
as
select 1 as user_id, 2 as level;


create table t2
engine = MergeTree()
order by tuple()
as
select 1 as user_id, 'website' as event_source, '2023-01-01 00:00:00'::DateTime as timestamp;


alter table t2
add column date Date alias toDate(timestamp);

SELECT
  any(t2.date) as any_val
FROM t1 AS t1
LEFT JOIN t2 as t2
  ON (t1.user_id = t2.user_id);
