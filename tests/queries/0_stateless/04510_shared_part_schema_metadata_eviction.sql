-- Eviction of the shared part metadata cache across ALTER ADD/DROP COLUMN cycles,
-- with and without Nested columns and with `share_nested_offsets` enabled and disabled
-- (the regression matrix of https://github.com/ClickHouse/ClickHouse/issues/102926).
--
-- Note: parts replaced by mutations stay alive as Outdated part objects until the old parts
-- cleanup runs, and each alive part object holds its cache entry. `old_parts_lifetime` is pinned
-- so that no cleanup happens during the test and the counts below are deterministic: they include
-- the entries held by Outdated parts and by part directories still on disk after DETACH/ATTACH.

drop table if exists t_evict_plain;
drop table if exists t_evict_nested;
drop table if exists t_evict_nested_noshare;

-- { echoOn }

create table t_evict_plain (key Int) engine = MergeTree order by key settings old_parts_lifetime = 480;
insert into t_evict_plain values (1);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_plain';
alter table t_evict_plain add column v1 String settings mutations_sync = 2;
insert into t_evict_plain values (2, '2');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_plain';
-- The dropped column set stays cached only while the pre-mutation parts are alive as Outdated.
alter table t_evict_plain drop column v1 settings mutations_sync = 2;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_plain';
alter table t_evict_plain add column v2 Nullable(Int64) settings mutations_sync = 2;
insert into t_evict_plain values (3, 3);
alter table t_evict_plain drop column v2 settings mutations_sync = 2;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_plain';
select count(), sum(key) from t_evict_plain;

create table t_evict_nested (key Int, `n.a` Array(Int32), `n.b` Array(String)) engine = MergeTree order by key settings old_parts_lifetime = 480;
insert into t_evict_nested values (1, [10], ['hello']);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested';
alter table t_evict_nested add column value String settings mutations_sync = 2;
insert into t_evict_nested values (2, [20], ['world'], '2');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested';
-- Detaching a part destroys its in-memory object synchronously: its cache entry must be evicted
-- (this was the leak in issue 102926: entries for schemas with a distinct collected-nested
-- description were never evicted).
alter table t_evict_nested detach part 'all_1_1_0';
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested';
detach table t_evict_nested;
attach table t_evict_nested;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested';
select count() from t_evict_nested;

create table t_evict_nested_noshare (key Int, `n.a` Array(Int32), `n.b` Array(String)) engine = MergeTree order by key settings share_nested_offsets = 0, old_parts_lifetime = 480;
insert into t_evict_nested_noshare values (1, [10], ['hello']);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested_noshare';
alter table t_evict_nested_noshare add column value String settings mutations_sync = 2;
insert into t_evict_nested_noshare values (2, [20], ['world'], '2');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested_noshare';
alter table t_evict_nested_noshare detach part 'all_1_1_0';
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_evict_nested_noshare';
select count() from t_evict_nested_noshare;

-- { echoOff }

drop table t_evict_plain;
drop table t_evict_nested;
drop table t_evict_nested_noshare;
