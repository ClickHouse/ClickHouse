-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts read counts in query_log for a single-node
--                       in-order merge.

-- The preliminary merges of a two-level in-order merge forward their members' virtual
-- rows downstream, so the top-level merge can defer whole groups. A limit answered at
-- the start of the key range must not read a block from every group.

create table tab (x UInt64, v UInt64) engine = MergeTree order by x;

system stop merges tab;

-- 32 parts of 100000 rows with disjoint key ranges in key order.
insert into tab select number, number from numbers(3200000)
settings max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

select count() from system.parts where database = currentDatabase() and table = 'tab' and active;

-- Force two-level merging with 16 groups of two parts.
select x from tab order by x limit 3
settings read_in_order_two_level_merge_threshold = 0, max_threads = 16,
         read_in_order_use_virtual_row = 1, use_query_condition_cache = 0,
         log_comment = '05044_two_level_lazy';

select x from tab order by x limit 3
settings read_in_order_two_level_merge_threshold = 0, max_threads = 16,
         read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         use_query_condition_cache = 0,
         log_comment = '05044_two_level_lazy_per_block';

system flush logs query_log;

-- Only the front group's front part contributes: its merge keeps a few 8192-row blocks in
-- flight when the limit stops the query, and the deferred groups add none. Without the
-- deferral every group reads 3-4 blocks (48-64 blocks here).
select read_rows <= 24 * 8192 from system.query_log
where current_database = currentDatabase() and log_comment = '05044_two_level_lazy'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

-- Per block, the announcement after the first block parks the front group as well.
select read_rows <= 3 * 8192 from system.query_log
where current_database = currentDatabase() and log_comment = '05044_two_level_lazy_per_block'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

drop table tab;
