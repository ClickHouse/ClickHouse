-- Tags: no-random-settings

-- Distributed aggregation uses a grouping-key NDV as a lower-bound estimate for the number of
-- groups. Only an NDV measured over exactly the produced rows provides that guarantee: after a
-- filter, the whole-table NDV is merely an upper bound and must be ignored in favor of row count.

SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET max_rows_to_group_by = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET distributed_plan_max_rows_to_broadcast = 100;
SET distributed_plan_default_shuffle_join_bucket_count = 3;
SET distributed_plan_default_reader_bucket_count = 3;

DROP TABLE IF EXISTS t_distributed_aggregation_ndv;
CREATE TABLE t_distributed_aggregation_ndv (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_distributed_aggregation_ndv SELECT number, number FROM numbers(1000);

SELECT '-- filtered NDV is ignored: partial aggregation';
SELECT
    countIf(explain LIKE '%MergingAggregated%'),
    countIf(explain LIKE '%ShuffleExchange%')
FROM
(
    EXPLAIN actions = 0, header = 0
    SELECT k, sum(v)
    FROM t_distributed_aggregation_ndv
    WHERE k < 10
    GROUP BY k
);

SELECT '-- exact NDV remains eligible: shuffle aggregation';
SELECT
    countIf(explain LIKE '%MergingAggregated%'),
    countIf(explain LIKE '%ShuffleExchange%')
FROM
(
    EXPLAIN actions = 0, header = 0
    SELECT k, sum(v)
    FROM t_distributed_aggregation_ndv
    GROUP BY k
);

DROP TABLE t_distributed_aggregation_ndv;
