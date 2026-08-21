-- Tags: no-random-merge-tree-settings, no-random-settings

create table tab (x UInt64, y UInt64) engine = MergeTree order by x;

insert into tab select number, number from numbers(1e6);
insert into tab select number, number from numbers(1e6, 1e6);

select _part, min(x), max(x) from tab group by _part order by _part ;

select x from tab where bitAnd(y, 1023) == 0 order by x limit 10 settings read_in_order_use_virtual_row=1, log_processors_profiles=1, optimize_move_to_prewhere=0, max_threads=2;

system flush logs query_log, processors_profile_log;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query like 'select x from tab%' AND event_date >= (today() - 1)
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT 
    replace(name, 'ReadPoolParallelReplicasInOrder', 'ReadPoolInOrder') AS name, output_rows
from system.processors_profile_log where event_date >= (today() - 1) and query_id = id
    and (name like '%MergeTreeSelect%' or name like '%VirtualRowTransform%')
ORDER BY name, output_rows;
