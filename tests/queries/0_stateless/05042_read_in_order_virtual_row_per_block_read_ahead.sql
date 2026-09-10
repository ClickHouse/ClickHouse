-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts read counts in query_log for a single-node
--                       in-order merge.

-- With `read_in_order_use_virtual_row_per_block` every source emits a virtual row after
-- each block, so between blocks a source is parked and woken again by the read-ahead in
-- `VirtualRowReadAheadTransform`. Exercise the re-deferral cycle over a scan that must
-- visit every part: the filter matches only at the very end of the key range, so the merge
-- crosses all sources and the read-ahead window keeps them reading. Also exercise the
-- credit accounting for fully-filtered blocks (a lane delivers a 0-row chunk, then its
-- next virtual row) by filtering out entire leading parts.

create table tab (x UInt64, v UInt8) engine = MergeTree order by x;

system stop merges tab;

-- Eight disjoint parts; only the last one contains matching rows.
insert into tab select number + 0 * 100000, 0 from numbers(100000);
insert into tab select number + 1 * 100000, 0 from numbers(100000);
insert into tab select number + 2 * 100000, 0 from numbers(100000);
insert into tab select number + 3 * 100000, 0 from numbers(100000);
insert into tab select number + 4 * 100000, 0 from numbers(100000);
insert into tab select number + 5 * 100000, 0 from numbers(100000);
insert into tab select number + 6 * 100000, 0 from numbers(100000);
insert into tab select number + 7 * 100000, if(number >= 99990, 1, 0) from numbers(100000);

select x from tab where v = 1 order by x limit 5
settings read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         max_threads = 2, use_query_condition_cache = 0, use_statistics_for_part_pruning = 0,
         log_comment = '05042_per_block_scan';

-- Same scan in reverse order: the matches are at the start of the reverse scan,
-- so only the tail of the last part must be read.
select x from tab where v = 1 order by x desc limit 5
settings read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         max_threads = 2, use_query_condition_cache = 0, use_statistics_for_part_pruning = 0,
         log_comment = '05042_per_block_scan_reverse';

system flush logs query_log;

-- The forward scan cannot stop early: every part must be read in full.
select read_rows from system.query_log
where current_database = currentDatabase() and log_comment = '05042_per_block_scan'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

-- The reverse scan is answered by the last part alone; the seven deferred parts stay unread.
select read_rows <= 100000 from system.query_log
where current_database = currentDatabase() and log_comment = '05042_per_block_scan_reverse'
    and type = 'QueryFinish' and event_date >= (today() - 1) and event_time >= now() - 600
order by event_time desc limit 1;

drop table tab;
