DROP TABLE IF EXISTS t_proj_external_agg;

CREATE TABLE t_proj_external_agg
(
    k1 UInt32,
    k2 UInt32,
    k3 UInt32,
    value UInt32
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_proj_external_agg SELECT 1, number%2, number%4, number FROM numbers(50000);

SYSTEM STOP MERGES t_proj_external_agg;

ALTER TABLE t_proj_external_agg ADD PROJECTION aaaa (
    SELECT
        k1,
        k2,
        k3,
        sum(value)
    GROUP BY k1, k2, k3
);

INSERT INTO t_proj_external_agg SELECT 1, number%2, number%4, number FROM numbers(100000) LIMIT 50000, 100000;

SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT '*** enable slowdown_parallel_replicas_local_plan_read ***';
SELECT k1, k2, k3, sum(value) v FROM t_proj_external_agg GROUP BY k1, k2, k3 ORDER BY k1, k2, k3 SETTINGS optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

DROP TABLE IF EXISTS t_proj_external_agg;
