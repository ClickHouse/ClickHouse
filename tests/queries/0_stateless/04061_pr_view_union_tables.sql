-- Test that parallel replicas work with a view containing UNION ALL of two MergeTree tables

DROP TABLE IF EXISTS t_rtb_hourly_1;
DROP TABLE IF EXISTS t_rtb_hourly_2;
DROP VIEW IF EXISTS v_rtb_union;

CREATE TABLE t_rtb_hourly_1
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

CREATE TABLE t_rtb_hourly_2
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

INSERT INTO t_rtb_hourly_1
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1
    FROM numbers(5000);

INSERT INTO t_rtb_hourly_2
    SELECT
        toDateTime('2024-01-01', 'UTC') + toIntervalHour(number % 720),
        number % 3,
        number % 20,
        number % 20,
        number % 5,
        1,
        1
    FROM numbers(5000);

CREATE VIEW v_rtb_union AS
    SELECT * FROM t_rtb_hourly_1
    UNION ALL
    SELECT * FROM t_rtb_hourly_2;

-- Baseline without parallel replicas
SELECT 'non-parallel';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_union
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET max_threads = 4; -- override random max_threads=1 which makes the correctness query too slow under sanitizers
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- Setting disabled — no distribution, plain Aggregating
SELECT '-- parallel, parallel_replicas_allow_view_over_mergetree = 0';
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
        FROM v_rtb_union
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 0
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_union
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 0;

-- UNION view — with setting enabled, walker follows first query and finds MergeTree, so query is distributed
SELECT '-- parallel, parallel_replicas_allow_view_over_mergetree = 1';
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
        FROM v_rtb_union
        WHERE ((Hour >= '2024-01-15') AND (Hour <= '2024-01-25')) AND (DeviceTypeId > 0)
        GROUP BY
            AppOrSiteIdDomain,
            DeviceTypeId
        ORDER BY
            AppOrSiteIdDomain ASC,
            DeviceTypeId ASC
        SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
    ))
)
WHERE e IN ('Aggregating', 'MergingAggregated');

SELECT '---';
SELECT
    BundleDomain AS AppOrSiteIdDomain,
    DeviceTypeId,
    sum(Impressions) AS Impressions,
    sum(Clicks) AS Clicks
FROM v_rtb_union
WHERE (Hour >= '2024-01-15' AND Hour <= '2024-01-25') AND DeviceTypeId > 0
GROUP BY AppOrSiteIdDomain, DeviceTypeId
ORDER BY ALL
SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_rtb_union;
DROP TABLE t_rtb_hourly_1;
DROP TABLE t_rtb_hourly_2;
