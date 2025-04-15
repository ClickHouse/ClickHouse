-- Tags: long, no-parallel
-- no-parallel: working with system.query_log

drop table if exists 03274_prewhere_is_deleted;

system stop merges;

create table 03274_prewhere_is_deleted (number UInt64, value String, version UInt64, is_deleted UInt8) engine=ReplacingMergeTree(version,is_deleted) ORDER BY number AS SELECT number, toString(number), 1, 1 FROM numbers(10000000);

optimize table 03274_prewhere_is_deleted final;

set do_not_merge_across_partitions_select_final=1;

truncate table system.query_log;

select count() from 03274_prewhere_is_deleted final SETTINGS log_comment='3274_final_query';

-- check that it works for queries that already have an explicit WHERE
select count() from 03274_prewhere_is_deleted final where number > 100 SETTINGS log_comment='3274_final_query';

-- check that it works for queries that already have an explicit PREWHERE
select count() from 03274_prewhere_is_deleted final prewhere number > 100 SETTINGS log_comment='3274_final_query';

system flush logs;

-- should be zero in case the optimization worked
SELECT sum(ProfileEvents['ReplacingSortedMilliseconds']) FROM system.query_log WHERE current_database = currentDatabase() AND type = 2 AND log_comment = '3274_final_query';

truncate table system.query_log;

drop table if exists 03274_prewhere_is_deleted;
