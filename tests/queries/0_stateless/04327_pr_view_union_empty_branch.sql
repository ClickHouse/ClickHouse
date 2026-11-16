-- Test that parallel replicas engages over a UNION ALL view even when the first branch
-- either reads from an empty MergeTree (its ReadFromMergeTree is replaced with ReadNothingStep)
-- or reads from a MergeTree whose index analysis prunes every part.
--
-- See `src/Planner/PlannerJoinTree.cpp` PR walker (uses findReadingSteps) and the
-- reading_steps.size() == 1 guard around the parallel_replicas_min_number_of_rows_per_replica check.

DROP TABLE IF EXISTS t_empty;
DROP TABLE IF EXISTS t_data1;
DROP TABLE IF EXISTS t_data2;
DROP TABLE IF EXISTS t_data3;
DROP VIEW IF EXISTS v_empty_first;
DROP VIEW IF EXISTS v_empty_second;
DROP VIEW IF EXISTS v_pruned_first;
DROP VIEW IF EXISTS v_pruned_second;

CREATE TABLE t_empty
(
    Hour DateTime('UTC'),
    NetworkId UInt16,
    BundleDomain Int32,
    AppSiteChannelId Int32,
    DeviceTypeId UInt16,
    Impressions UInt64,
    Clicks UInt64,
)
ENGINE = MergeTree()
PARTITION BY toYearWeek(Hour)
ORDER BY (Hour, NetworkId, BundleDomain, AppSiteChannelId, DeviceTypeId)
SETTINGS index_granularity=10;

CREATE TABLE t_data1
(
    Hour DateTime('UTC'),
    NetworkId UInt16,
    BundleDomain Int32,
    AppSiteChannelId Int32,
    DeviceTypeId UInt16,
    Impressions UInt64,
    Clicks UInt64,
)
ENGINE = MergeTree()
PARTITION BY toYearWeek(Hour)
ORDER BY (Hour, NetworkId, BundleDomain, AppSiteChannelId, DeviceTypeId)
SETTINGS index_granularity=10;

CREATE TABLE t_data2
(
    Hour DateTime('UTC'),
    NetworkId UInt16,
    BundleDomain Int32,
    AppSiteChannelId Int32,
    DeviceTypeId UInt16,
    Impressions UInt64,
    Clicks UInt64,
)
ENGINE = MergeTree()
PARTITION BY toYearWeek(Hour)
ORDER BY (Hour, NetworkId, BundleDomain, AppSiteChannelId, DeviceTypeId)
SETTINGS index_granularity=10;

-- t_data3 holds the same data as t_data1/t_data2; the views that reference it apply an inline
-- WHERE Hour < '1900-01-01' so MergeTree index analysis prunes every part of that branch to zero ranges.
CREATE TABLE t_data3
(
    Hour DateTime('UTC'),
    NetworkId UInt16,
    BundleDomain Int32,
    AppSiteChannelId Int32,
    DeviceTypeId UInt16,
    Impressions UInt64,
    Clicks UInt64,
)
ENGINE = MergeTree()
PARTITION BY toYearWeek(Hour)
ORDER BY (Hour, NetworkId, BundleDomain, AppSiteChannelId, DeviceTypeId)
SETTINGS index_granularity=10;

INSERT INTO t_data1
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1
    FROM numbers(5000);

INSERT INTO t_data2
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1
    FROM numbers(5000);

INSERT INTO t_data3
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1
    FROM numbers(5000);

CREATE VIEW v_empty_first AS
    SELECT * FROM t_empty
    UNION ALL
    SELECT * FROM t_data1
    UNION ALL
    SELECT * FROM t_data2;

CREATE VIEW v_empty_second AS
    SELECT * FROM t_data1
    UNION ALL
    SELECT * FROM t_empty
    UNION ALL
    SELECT * FROM t_data2;

CREATE VIEW v_pruned_first AS
    SELECT * FROM t_data3 WHERE Hour < '1900-01-01'
    UNION ALL
    SELECT * FROM t_data1
    UNION ALL
    SELECT * FROM t_data2;

CREATE VIEW v_pruned_second AS
    SELECT * FROM t_data1
    UNION ALL
    SELECT * FROM t_data3 WHERE Hour < '1900-01-01'
    UNION ALL
    SELECT * FROM t_data2;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET max_threads = 4; -- override random max_threads=1 which makes the correctness query too slow under sanitizers
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET explain_query_plan_default = 'legacy';

SELECT '-- v_empty_first';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_empty_first
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_empty_first
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT '-- v_empty_second';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_empty_second
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_empty_second
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT '-- v_pruned_first, parallel_replicas_min_number_of_rows_per_replica = 0';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_pruned_first
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_pruned_first
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT '-- v_pruned_first, parallel_replicas_min_number_of_rows_per_replica = 1';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_pruned_first
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 1
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_pruned_first
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 1;

SELECT '-- v_pruned_second, parallel_replicas_min_number_of_rows_per_replica = 0';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_pruned_second
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_pruned_second
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 0;

SELECT '-- v_pruned_second, parallel_replicas_min_number_of_rows_per_replica = 1';
SELECT trimLeft(explain) AS e
FROM
(
    SELECT *
    FROM viewExplain('EXPLAIN', '', (
        SELECT
            BundleDomain AS AppOrSiteIdDomain,
            DeviceTypeId,
            sum(Impressions) AS Impressions,
            sum(Clicks) AS Clicks
        FROM v_pruned_second
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 1
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_pruned_second
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_min_number_of_rows_per_replica = 1;

DROP VIEW v_pruned_second;
DROP VIEW v_pruned_first;
DROP VIEW v_empty_second;
DROP VIEW v_empty_first;
DROP TABLE t_data3;
DROP TABLE t_data2;
DROP TABLE t_data1;
DROP TABLE t_empty;
