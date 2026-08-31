-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts read counts in query_log for a single-node
--                       in-order merge.

-- The preliminary merges of a two-level in-order merge forward their members' virtual
-- rows downstream, so the top-level merge can defer whole groups. A limit answered at
-- the start of the key range must not read a block from every group.

create table tab (x UInt64, v UInt64) engine = MergeTree order by x;

system stop merges tab;

insert into tab select number + 0 * 100000, number from numbers(100000);
insert into tab select number + 1 * 100000, number from numbers(100000);
insert into tab select number + 2 * 100000, number from numbers(100000);
insert into tab select number + 3 * 100000, number from numbers(100000);
insert into tab select number + 4 * 100000, number from numbers(100000);
insert into tab select number + 5 * 100000, number from numbers(100000);
insert into tab select number + 6 * 100000, number from numbers(100000);
insert into tab select number + 7 * 100000, number from numbers(100000);

-- Force two-level merging with several groups.
select x from tab order by x limit 3
settings read_in_order_two_level_merge_threshold = 0, max_threads = 4,
         read_in_order_use_virtual_row = 1, use_query_condition_cache = 0,
         log_comment = '05044_two_level_lazy';

select x from tab order by x limit 3
settings read_in_order_two_level_merge_threshold = 0, max_threads = 4,
         read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         use_query_condition_cache = 0,
         log_comment = '05044_two_level_lazy_per_block';

system flush logs query_log;

-- Both modes: only the front group's front part contributes; the deferred groups must
-- stay (nearly) unread. Allow the read-ahead window one block per group.
select read_rows <= 100000 from system.query_log
where current_database = currentDatabase() and log_comment = '05044_two_level_lazy'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

select read_rows <= 100000 from system.query_log
where current_database = currentDatabase() and log_comment = '05044_two_level_lazy_per_block'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

drop table tab;
