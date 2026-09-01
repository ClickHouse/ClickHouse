-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- no-parallel-replicas: the test asserts per-source read counts in processors_profile_log
--                       for a single-node in-order merge; with parallel replicas the
--                       coordinator splits the read into ranges across sources, so the
--                       front/deferred source distinction does not apply.

-- Regression test for `read_in_order_use_virtual_row` combined with
-- `read_in_order_use_buffering`. A filtered `ORDER BY pk LIMIT n` query cannot push the
-- limit into reading, so `BufferChunksTransform` is inserted before the merge. Buffering
-- must not defeat the deferral of the sources behind virtual rows: after delivering a
-- virtual row, `BufferChunksTransform` must not read ahead into its buffer until the
-- merge actually demands data from that source. Otherwise every deferred source would
-- read speculatively regardless of the read-ahead window, inflating reads and peak
-- memory on filtered `LIMIT` queries.

create table tab (x UInt64, v UInt8) engine = MergeTree order by x;

system stop merges tab;

-- Eight disjoint parts: the merge answers `ORDER BY x LIMIT 99000` from the first part
-- alone and never advances past a source, so no read-ahead is issued and the seven
-- deferred sources must stay completely unread even though buffering is enabled.
insert into tab select number + 0 * 100000, 1 from numbers(100000);
insert into tab select number + 1 * 100000, 1 from numbers(100000);
insert into tab select number + 2 * 100000, 1 from numbers(100000);
insert into tab select number + 3 * 100000, 1 from numbers(100000);
insert into tab select number + 4 * 100000, 1 from numbers(100000);
insert into tab select number + 5 * 100000, 1 from numbers(100000);
insert into tab select number + 6 * 100000, 1 from numbers(100000);
insert into tab select number + 7 * 100000, 1 from numbers(100000);

select x from tab where v = 1 order by x limit 99000 format Null
settings read_in_order_use_virtual_row = 1, read_in_order_use_buffering = 1,
         optimize_move_to_prewhere = 0, log_processors_profiles = 1, max_threads = 2;

system flush logs query_log, processors_profile_log;

-- Assert that the buffered path was actually exercised (`BufferChunks` present), that
-- every part engaged the virtual row, that only the front source delivered data, and
-- that the seven deferred sources read nothing at all.
WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query like 'select x from tab where v = 1 order by x limit 99000%' AND event_date >= (today() - 1) AND event_time >= now() - 600
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT
    countIf(name like '%VirtualRowTransform%') AS virtual_rows,
    countIf(name = 'BufferChunks') AS buffer_transforms,
    countIf(name like '%MergeTreeSelect%' and output_rows > 0) AS sources_delivering_data,
    countIf(name like '%MergeTreeSelect%' and output_rows = 0) AS deferred_sources_left_unread
from system.processors_profile_log where event_date >= (today() - 1) and query_id = id
    and (name like '%MergeTreeSelect%' or name like '%VirtualRowTransform%' or name = 'BufferChunks');

drop table tab;
