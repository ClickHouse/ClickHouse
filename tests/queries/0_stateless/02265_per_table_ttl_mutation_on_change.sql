drop table if exists per_table_ttl_02265;
create table per_table_ttl_02265 (key Int, date Date, value String) engine=MergeTree() order by key;
insert into per_table_ttl_02265 values (1, today(), '1');

-- { echoOn }
alter table per_table_ttl_02265 modify TTL date + interval 1 month;
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 1 month;
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 2 month;
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 2 month group by key set value = argMax(value, date);
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 2 month group by key set value = argMax(value, date);
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 2 month recompress codec(ZSTD(17));
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';
alter table per_table_ttl_02265 modify TTL date + interval 2 month recompress codec(ZSTD(17));
select count() from system.mutations where database = currentDatabase() and table = 'per_table_ttl_02265';

-- { echoOff }
drop table per_table_ttl_02265;
