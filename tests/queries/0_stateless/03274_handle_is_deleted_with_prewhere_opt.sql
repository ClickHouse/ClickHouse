drop table if exists 03274_prewhere_is_deleted;

create table 03274_prewhere_is_deleted (number UInt64, value String, version UInt64, is_deleted UInt8) engine=ReplacingMergeTree(version,is_deleted) ORDER BY number AS SELECT number, toString(number), 1, 1 FROM numbers(10000000);

optimize table 03274_prewhere_is_deleted final;

set do_not_merge_across_partitions_select_final=1;

truncate table system.query_log;

select count() from 03274_prewhere_is_deleted final SETTINGS log_comment='final_query';

system flush logs;

-- should be zero in case the optimization worked
SELECT ProfileEvents['ReplacingSortedMilliseconds'] FROM system.query_log WHERE event_time > now() - 600 and log_comment = 'final_query';

drop table if exists 03274_prewhere_is_deleted;
