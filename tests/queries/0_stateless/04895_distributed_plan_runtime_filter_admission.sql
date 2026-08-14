-- Tags: no-old-analyzer

CREATE TABLE t_small (sid UInt64) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE t_large (lid UInt64) ENGINE = MergeTree ORDER BY lid;
INSERT INTO t_small SELECT number * 100 FROM numbers(100);
INSERT INTO t_large SELECT number FROM numbers(1000000);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_join_runtime_filters = 1;

-- More estimated build keys than the probe site has rows: shipping the filter costs at least as
-- much as it could ever save, so transport is refused and the local build step stays.
SELECT '-- build side larger than the probe site: transport refused';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM t_small, t_large WHERE sid = lid
) WHERE explain LIKE '%RuntimeFilter%';
SELECT count() FROM t_small, t_large WHERE sid = lid;

SELECT '-- small build side against a large probe site: transport admitted';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM t_large, t_small WHERE lid = sid
) WHERE explain LIKE '%RuntimeFilter%';
SELECT count() FROM t_large, t_small WHERE lid = sid;
