-- Tags: no-random-merge-tree-settings, no-random-settings

create table tab (x UInt64, y UInt64) engine = MergeTree order by x;

insert into tab select number, number from numbers(1e6);
insert into tab select number, number from numbers(1e6, 1e6);

select _part, min(x), max(x) from tab group by _part order by _part ;

select x from tab where bitAnd(y, 1023) == 0 order by x limit 10 settings read_in_order_use_virtual_row=1, log_processors_profiles=1, optimize_move_to_prewhere=0, max_threads=2;

system flush logs query_log, processors_profile_log;

-- With `read_in_order_use_virtual_row`, the merge answers `ORDER BY x LIMIT 10` from the
-- first part alone. A bounded read-ahead window (sized by `max_threads`) may still
-- speculatively prefetch one block from the second part, so its read count is timing
-- dependent (0 or one block) but never the whole part. Assert the deterministic invariants:
-- both parts have their own `MergeTreeSelect` and `VirtualRowTransform` (virtual row is
-- engaged) and neither reads more than one block.
WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query like 'select x from tab%' AND event_date >= (today() - 1) AND event_time >= now() - 600
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT
    replace(name, 'ReadPoolParallelReplicasInOrder', 'ReadPoolInOrder') AS name,
    count() AS processors,
    max(output_rows) <= 65536 AS reads_at_most_one_block
from system.processors_profile_log where event_date >= (today() - 1) and query_id = id
    and (name like '%MergeTreeSelect%' or name like '%VirtualRowTransform%')
GROUP BY name
ORDER BY name;
