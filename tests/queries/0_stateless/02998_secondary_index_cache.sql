-- tags: no-parallel
--       ^ because we look at server-wide metrics

system drop secondary index cache;
select metric, value from system.metrics where metric = 'SecondaryIndexCacheSize';

create table a (k Int64, v Int64, index i v type set(0) granularity 1) engine MergeTree order by k settings index_granularity = 128;
insert into a select number, number * 10 from numbers(1280);
select metric, value from system.metrics where metric = 'SecondaryIndexCacheSize';

select count() from a where v = 25;
select metric, value >= 1280 * 8, value < 1280 * 8 * 2 from system.metrics where metric = 'SecondaryIndexCacheSize';

select count() from a where v = 30;
select metric, value >= 1280 * 8, value < 1280 * 8 * 2 from system.metrics where metric = 'SecondaryIndexCacheSize';

system flush logs;

select query, ProfileEvents['SecondaryIndexCacheHits'], ProfileEvents['SecondaryIndexCacheMisses'] from system.query_log where event_date >= yesterday() and current_database = currentDatabase() and type = 'QueryFinish' and query like 'select count()%' order by event_time_microseconds;
