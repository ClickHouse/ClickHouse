-- Two-level view nesting.
-- Base tables -> UNION ALL view (zeroed cross-metrics) -> view-on-view -> query.

DROP TABLE IF EXISTS t_ssp;
DROP TABLE IF EXISTS t_dsp;
DROP VIEW IF EXISTS v_dashboard;
DROP VIEW IF EXISTS dv_dashboard;

CREATE TABLE t_ssp
(
    hour DateTime('UTC'),
    network_id UInt16,
    deal_id Int32,
    child_deal_id Int32,
    impressions UInt64,
    clicks UInt64
) ENGINE = MergeTree ORDER BY (network_id, hour);

CREATE TABLE t_dsp
(
    hour DateTime('UTC'),
    network_id UInt16,
    deal_id Int32,
    child_deal_id Int32,
    bids UInt64,
    bid_requests UInt64
) ENGINE = MergeTree ORDER BY (network_id, hour);

CREATE VIEW v_dashboard AS
SELECT
    hour, network_id, deal_id, child_deal_id,
    impressions, clicks,
    0 AS bids, 0 AS bid_requests
FROM t_ssp
UNION ALL
SELECT
    hour, network_id, deal_id, child_deal_id,
    0 AS impressions, 0 AS clicks,
    bids, bid_requests
FROM t_dsp;

CREATE VIEW dv_dashboard AS SELECT * FROM v_dashboard;

-- Target rows spread across 15 days, network_id=3050, child_deal_id=200.
INSERT INTO t_ssp SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    100 + number % 50, 5 + number % 10
FROM numbers(10000);

-- Noise: other networks and deals.
INSERT INTO t_ssp SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 100, number % 20
FROM numbers(1000);

INSERT INTO t_dsp SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    10 + number % 20, 500 + number % 200
FROM numbers(10000);

-- Noise for DSP side.
INSERT INTO t_dsp SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 50, number % 500
FROM numbers(1000);

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- Check that the outermost view (dv_dashboard) is sent to parallel replicas,
-- not the inner v_dashboard or the base tables.
SELECT '-- plan check: outermost view sent to replicas';
SELECT if(explain LIKE '%dv_dashboard%', 'dv_dashboard', if(explain LIKE '%v_dashboard%', 'v_dashboard', if(explain LIKE '%t_ssp%' OR explain LIKE '%t_dsp%', 'base_table', 'other')))
FROM viewExplain('EXPLAIN', '', (
    SELECT
        sum(bids) AS Bids,
        sum(bid_requests) AS BidRequests,
        sum(impressions) AS Impressions,
        sum(clicks) AS Clicks,
        toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE
        hour >= toDateTime('2026-01-01 00:00:00', 'Europe/Paris')
        AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(child_deal_id != 0, child_deal_id, deal_id) IN (200)
        AND network_id IN (3050)
    GROUP BY Day
    HAVING Impressions != 0 OR Bids != 0
    ORDER BY Day ASC
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Correctness: EXCEPT should produce zero rows.
SELECT '-- correctness';
(
    SELECT sum(bids) AS Bids, sum(bid_requests) AS BidRequests, sum(impressions) AS Impressions, sum(clicks) AS Clicks, toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE hour >= toDateTime('2026-01-01 00:00:00', 'Europe/Paris') AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(child_deal_id != 0, child_deal_id, deal_id) IN (200) AND network_id IN (3050)
    GROUP BY Day HAVING Impressions != 0 OR Bids != 0
    ORDER BY ALL
    SETTINGS parallel_replicas_allow_view_over_mergetree = 0
)
EXCEPT
(
    SELECT sum(bids) AS Bids, sum(bid_requests) AS BidRequests, sum(impressions) AS Impressions, sum(clicks) AS Clicks, toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE hour >= toDateTime('2026-01-01 00:00:00', 'Europe/Paris') AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(child_deal_id != 0, child_deal_id, deal_id) IN (200) AND network_id IN (3050)
    GROUP BY Day HAVING Impressions != 0 OR Bids != 0
    ORDER BY ALL
    SETTINGS parallel_replicas_allow_view_over_mergetree = 1
);

DROP VIEW dv_dashboard;
DROP VIEW v_dashboard;
DROP TABLE t_dsp;
DROP TABLE t_ssp;
