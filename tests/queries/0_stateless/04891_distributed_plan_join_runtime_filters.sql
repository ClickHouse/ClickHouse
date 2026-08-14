-- Tags: no-old-analyzer

CREATE TABLE big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid;
CREATE TABLE small (sid UInt64, name String) ENGINE = MergeTree ORDER BY sid;
INSERT INTO big SELECT number, number FROM numbers(100000);
INSERT INTO small SELECT number * 100, toString(number) FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;

SELECT '-- shuffle join, setting off';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN actions=1 SELECT count() FROM big, small WHERE bid = sid
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%JoinLogical%' OR explain LIKE '%Filter id%' OR explain LIKE '%__applyFilter%';
SELECT count() FROM big, small WHERE bid = sid;

SELECT '-- shuffle join, setting on';
SET distributed_plan_join_runtime_filters = 1;
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN actions=1 SELECT count() FROM big, small WHERE bid = sid
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%JoinLogical%' OR explain LIKE '%Filter id%' OR explain LIKE '%__applyFilter%';
SELECT count() FROM big, small WHERE bid = sid;

SELECT '-- broadcast join, setting on';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN actions=1 SELECT count() FROM big, small WHERE bid = sid SETTINGS distributed_plan_max_rows_to_broadcast = 20000
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%JoinLogical%' OR explain LIKE '%Filter id%' OR explain LIKE '%__applyFilter%';
SELECT count() FROM big, small WHERE bid = sid SETTINGS distributed_plan_max_rows_to_broadcast = 20000;

SELECT '-- multiple keys, setting on';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM big, small WHERE bid = sid AND v = sid
) WHERE explain LIKE '%SendRuntimeFilter%' OR explain LIKE '%ReceiveRuntimeFilter%';
SELECT count() FROM big, small WHERE bid = sid AND v = sid;

SELECT '-- empty build side, setting on';
SELECT count() FROM big, small WHERE bid = sid AND name = 'no such name';

SELECT '-- anti join keeps its local filter';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN actions=1 SELECT count() FROM big LEFT ANTI JOIN small ON bid = sid
) WHERE explain LIKE '%RuntimeFilter%' OR explain LIKE '%Exchange%' OR explain LIKE '%JoinLogical%' OR explain LIKE '%Filter id%' OR explain LIKE '%__applyFilter%';
SELECT count() FROM big LEFT ANTI JOIN small ON bid = sid;
