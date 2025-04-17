-- Tags: long, no-parallel
-- no-parallel: working with system.query_log

-- Test for https://github.com/ClickHouse/ClickHouse/pull/76978

drop table if exists 03274_prewhere_is_deleted;

set optimize_on_insert=0;
set mutations_sync=2;

create table 03274_prewhere_is_deleted (number UInt64, value String, version UInt64, is_deleted UInt8) engine=ReplacingMergeTree(version,is_deleted) ORDER BY number;

INSERT INTO 03274_prewhere_is_deleted SELECT number, toString(number), 1, 1 FROM numbers(10000);

INSERT INTO 03274_prewhere_is_deleted SELECT number, toString(number), 1, 1 FROM numbers(10000);

optimize table 03274_prewhere_is_deleted final;

set do_not_merge_across_partitions_select_final=1;

select count() from 03274_prewhere_is_deleted final SETTINGS log_comment='3274_final_query';

-- check that it works for queries that already have an explicit WHERE
select count() from 03274_prewhere_is_deleted final where number > 100 SETTINGS log_comment='3274_final_query';

-- check that it works for queries that already have an explicit PREWHERE
select count() from 03274_prewhere_is_deleted final prewhere number > 100 SETTINGS log_comment='3274_final_query';

drop table if exists 03274_prewhere_is_deleted;
