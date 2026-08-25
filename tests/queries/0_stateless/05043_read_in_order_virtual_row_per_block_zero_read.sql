-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts per-source read counts in processors_profile_log
--                       for a single-node in-order merge; with parallel replicas the
--                       coordinator splits the read into ranges across sources, so the
--                       front/deferred source distinction does not apply.

-- Mirror of 04617_read_in_order_virtual_row_front_source_limit with
-- `read_in_order_use_virtual_row_per_block` enabled: even though the front source is
-- re-parked behind a virtual row after every block, the merge answers
-- `ORDER BY x LIMIT 100000` from it alone across multiple blocks, and the read-ahead
-- must not touch the deferred source.

create table tab (x UInt64) engine = MergeTree order by x;

system stop merges tab;

insert into tab select number from numbers(200000);
insert into tab select number from numbers(200000, 200000);

select x from tab order by x limit 100000 format Null
settings read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1,
         log_processors_profiles = 1, max_threads = 2;

system flush logs query_log, processors_profile_log;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query like 'select x from tab order by x limit 100000%' AND event_date >= (today() - 1) AND event_time >= now() - 600
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT
    countIf(name like '%VirtualRowTransform%') AS virtual_rows,
    countIf(name = 'VirtualRowReadAhead') AS read_ahead_transforms,
    countIf(name like '%MergeTreeSelect%' and output_rows >= 100000) AS front_sources_reading_past_the_limit,
    countIf(name like '%MergeTreeSelect%' and output_rows = 0) AS deferred_sources_left_unread
from system.processors_profile_log where event_date >= (today() - 1) and query_id = id
    and (name like '%MergeTreeSelect%' or name like '%VirtualRowTransform%' or name = 'VirtualRowReadAhead');

drop table tab;
