-- { echoOn }

drop table if exists x;

create table x (dt DateTime, i Int32) engine MergeTree partition by indexHint(dt) order by dt TTL dt + toIntervalDay(15) settings index_granularity = 8192;

insert into x values (now(), 1), (now() - toIntervalDay(30), 2);

optimize table x final;

select i from x;

select minmax_dt.1 == minmax_dt.2 from mergeTreeIndex(currentDatabase(), x, with_minmax = 1);

select (select min(dt) from x) == (select minDistinct(dt) from x);

drop table x;
