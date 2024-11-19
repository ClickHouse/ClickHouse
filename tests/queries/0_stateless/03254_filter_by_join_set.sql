-- Tags: long

create table numbers_10m (number UInt64, s String) engine = MergeTree order by number settings index_granularity=8192, index_granularity_bytes=10000000;
insert into numbers_10m select number, toString(number) from numbers_mt(10e6) settings max_insert_threads=8;

-- explain actions = 1 select number, s, k, v from numbers_10m inner join (select number * 100 + 5000000 as k, toString(number) || '_r' as v from numbers(20)) as r on number = r.k order by number;


set merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0;



-- first_query
select number, s, k, v from numbers_10m inner join (select number * 100 + 5000000 as k, toString(number) || '_r' as v from numbers(20)) as r on number = r.k order by number;

select trimLeft(explain) from (EXPLAIN actions = 1 select number, s, k, v from numbers_10m inner join (select number * 100 + 5000000 as k, toString(number) || '_r' as v from numbers(20)) as r on number = r.k order by number settings enable_analyzer=1) where explain like '%Join%' or explain like '%Dynamic Filter%' or explain like '%FUNCTION in%';

system flush logs;

select if(read_rows < 8192 * 3, 0, read_rows) from system.query_log where event_date >= today() - 1 and current_database = currentDatabase() and query like '-- first_query%' and type = 'QueryFinish';
--select * from system.query_log where event_date >= today() - 1 and current_database = currentDatabase() and query like '-- first_query%' and type = 'QueryFinish' format Vertical;

-- second_query
select number, s, k, k2, v from numbers_10m inner join (select number * 100 + 5000000 as k, number * 10 + 4000000 as k2, toString(number) || '_r' as v from numbers(20)) as r on number = r.k or number = r.k2 order by number;
select trimLeft(explain) from (EXPLAIN actions = 1 select number, s, k, k2, v from numbers_10m inner join (select number * 100 + 5000000 as k, number * 10 + 4000000 as k2, toString(number) || '_r' as v from numbers(20)) as r on number = r.k or number = r.k2 order by number) where explain like '%Join%' or explain like '%Dynamic Filter%' or explain like '%FUNCTION in%';

select if(read_rows < 8192 * 6, 0, read_rows) from system.query_log where event_date >= today() - 1 and current_database = currentDatabase() and query like '-- first_query%' and type = 'QueryFinish';
