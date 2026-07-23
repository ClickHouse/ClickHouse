-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102926
-- `decrefColumnsDescriptionForColumns` must evict cache entries for schemas with
-- `Nested` columns where `Nested::collect` produces a distinct `with_collected_nested`.

drop table if exists t_nested_leak;

-- { echoOn }
create table t_nested_leak (key Int, `n.a` Array(Int32), `n.b` Array(String)) engine=MergeTree() order by key;
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_nested_leak';
insert into t_nested_leak values (1, [10], ['hello']);
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_nested_leak';
alter table t_nested_leak add column value String settings mutations_sync=2;
insert into t_nested_leak values (10, [30], ['!'], '10');
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_nested_leak';
alter table t_nested_leak detach part 'all_1_1_0';
select columns_descriptions_cache_size from system.tables where database = currentDatabase() and table = 't_nested_leak';
-- { echoOff }

drop table t_nested_leak;
