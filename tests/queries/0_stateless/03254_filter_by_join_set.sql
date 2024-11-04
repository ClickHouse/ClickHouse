-- Tags: long

create table numbers_10m (number UInt64, s String) engine = MergeTree order by number settings index_granularity=8192, index_granularity_bytes=10000000;
insert into numbers_10m select number, toString(number) from numbers_mt(10e6) settings max_insert_threads=8;

-- explain actions = 1 select number, s, k, v from numbers_10m inner join (select number * 100 + 5000000 as k, toString(number) || '_r' as v from numbers(20)) as r on number = r.k order by number;


set optimize_read_in_order=0;



-- first_query
select number, s, k, v from numbers_10m inner join (select number * 100 + 5000000 as k, toString(number) || '_r' as v from numbers(20)) as r on number = r.k order by number;

system flush logs;

select if(read_rows < 8192 * 3, 0, read_rows) from system.query_log where event_date >= today() - 1 and current_database = currentDatabase() and query like '-- first_query%' and type = 'QueryFinish';
