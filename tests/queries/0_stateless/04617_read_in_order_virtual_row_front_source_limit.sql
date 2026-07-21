-- Tags: no-random-merge-tree-settings, no-random-settings

create table tab (x UInt64) engine = MergeTree order by x;

system stop merges tab;

-- Two disjoint parts: the merge answers `ORDER BY x LIMIT 100000` from the first part alone,
-- but only after consuming two blocks from it (`max_block_size = 65536` < 100000 < 131072).
insert into tab select number from numbers(200000);
insert into tab select number from numbers(200000, 200000);

select _part, min(x), max(x) from tab group by _part order by _part;

select x from tab order by x limit 100000 format Null settings read_in_order_use_virtual_row=1, log_processors_profiles=1, max_threads=2;

system flush logs query_log, processors_profile_log;

-- The read-ahead window must stay unarmed while the merge keeps asking the *same* front
-- source for more blocks: asking it again after its first block is not an advance past a
-- source. Assert that both parts engage the virtual row (two `VirtualRowTransform`), that
-- exactly one `MergeTreeSelect` delivers the limit (the front part, at least two blocks),
-- and that the other reads nothing at all.
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
    countIf(name like '%MergeTreeSelect%' and output_rows >= 100000) AS front_sources_reading_past_the_limit,
    countIf(name like '%MergeTreeSelect%' and output_rows = 0) AS deferred_sources_left_unread
from system.processors_profile_log where event_date >= (today() - 1) and query_id = id
    and (name like '%MergeTreeSelect%' or name like '%VirtualRowTransform%');
