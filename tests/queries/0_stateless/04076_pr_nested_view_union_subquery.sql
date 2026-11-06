-- Tags: long

-- Three-level view nesting mirroring a customer's schema:
-- Archive + current tables (different column names) -> D_SSP/D_DSP views
--   (inner UNION ALL subquery with column renames + outer SELECT * with backward-compat aliases)
-- -> UNION ALL view (zeroed cross-metrics) -> view-on-view -> query.

DROP TABLE IF EXISTS t_ssp_archive;
DROP TABLE IF EXISTS t_ssp_current;
DROP TABLE IF EXISTS t_dsp_archive;
DROP TABLE IF EXISTS t_dsp_current;
DROP VIEW IF EXISTS d_ssp;
DROP VIEW IF EXISTS d_dsp;
DROP VIEW IF EXISTS v_dashboard;
DROP VIEW IF EXISTS dv_dashboard;

-- Archive tables use old column names.
CREATE TABLE t_ssp_archive
(
    hour DateTime('UTC'),
    publisher_id UInt16,
    deal_id Int32,
    child_deal_id Int32,
    impressions UInt64,
    clicks UInt64
) ENGINE = MergeTree ORDER BY (publisher_id, hour) SETTINGS index_granularity = 10;

-- Current tables use new column names.
CREATE TABLE t_ssp_current
(
    hour DateTime('UTC'),
    network_id UInt16,
    internal_deal_id Int32,
    internal_child_deal_id Int32,
    impressions UInt64,
    clicks UInt64
) ENGINE = MergeTree ORDER BY (network_id, hour) SETTINGS index_granularity = 10;

CREATE TABLE t_dsp_archive
(
    hour DateTime('UTC'),
    publisher_id UInt16,
    deal_id Int32,
    child_deal_id Int32,
    bid_count UInt64,
    bid_request_partner UInt64
) ENGINE = MergeTree ORDER BY (publisher_id, hour) SETTINGS index_granularity = 10;

CREATE TABLE t_dsp_current
(
    hour DateTime('UTC'),
    network_id UInt16,
    internal_deal_id Int32,
    internal_child_deal_id Int32,
    bids UInt64,
    bid_requests UInt64
) ENGINE = MergeTree ORDER BY (network_id, hour) SETTINGS index_granularity = 10;

-- D_SSP view: inner UNION ALL normalizes column names, outer SELECT * adds backward-compat aliases.
CREATE VIEW d_ssp AS
SELECT
    *,
    network_id AS publisher_id,
    internal_deal_id AS deal_id,
    internal_child_deal_id AS child_deal_id
FROM
(
    SELECT
        hour,
        publisher_id AS network_id,
        deal_id AS internal_deal_id,
        child_deal_id AS internal_child_deal_id,
        impressions,
        clicks
    FROM t_ssp_archive
    WHERE hour <= '2025-06-01 00:00:00'
    UNION ALL
    SELECT
        hour,
        network_id,
        internal_deal_id,
        internal_child_deal_id,
        impressions,
        clicks
    FROM t_ssp_current
    WHERE hour > '2025-06-01 00:00:00'
);

-- D_DSP view: same pattern for the DSP side.
CREATE VIEW d_dsp AS
SELECT
    *,
    network_id AS publisher_id,
    internal_deal_id AS deal_id,
    internal_child_deal_id AS child_deal_id,
    bids AS bid_count,
    bid_requests AS bid_request_partner
FROM
(
    SELECT
        hour,
        publisher_id AS network_id,
        deal_id AS internal_deal_id,
        child_deal_id AS internal_child_deal_id,
        bid_count AS bids,
        bid_request_partner AS bid_requests
    FROM t_dsp_archive
    WHERE hour <= '2025-06-01 00:00:00'
    UNION ALL
    SELECT
        hour,
        network_id,
        internal_deal_id,
        internal_child_deal_id,
        bids,
        bid_requests
    FROM t_dsp_current
    WHERE hour > '2025-06-01 00:00:00'
);

-- V_dashboard: UNION ALL of SSP (zeroed DSP metrics) and DSP (zeroed SSP metrics).
CREATE VIEW v_dashboard AS
SELECT
    hour, network_id, internal_deal_id, internal_child_deal_id,
    impressions, clicks,
    0 AS bids, 0 AS bid_requests
FROM d_ssp
UNION ALL
SELECT
    hour, network_id, internal_deal_id, internal_child_deal_id,
    0 AS impressions, 0 AS clicks,
    bids, bid_requests
FROM d_dsp;

-- View-on-view.
CREATE VIEW dv_dashboard AS SELECT * FROM v_dashboard;

-- Archive data: spread across 15 days before switch date, network_id=3050, child_deal_id=200.
INSERT INTO t_ssp_archive SELECT
    toDateTime('2025-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    100 + number % 50, 5 + number % 10
FROM numbers(10000);

-- Archive noise.
INSERT INTO t_ssp_archive SELECT
    toDateTime('2025-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 100, number % 20
FROM numbers(1000);

INSERT INTO t_dsp_archive SELECT
    toDateTime('2025-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    10 + number % 20, 500 + number % 200
FROM numbers(10000);

INSERT INTO t_dsp_archive SELECT
    toDateTime('2025-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 50, number % 500
FROM numbers(1000);

-- Current data: spread across 15 days after switch date, network_id=3050.
INSERT INTO t_ssp_current SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    50 + number % 30, 2 + number % 8
FROM numbers(5000);

INSERT INTO t_ssp_current SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 100, number % 20
FROM numbers(1000);

INSERT INTO t_dsp_current SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalDay(number % 15) + toIntervalHour(number % 24),
    3050, 100, 200,
    5 + number % 15, 300 + number % 100
FROM numbers(5000);

INSERT INTO t_dsp_current SELECT
    toDateTime('2026-01-01 00:00:00', 'UTC') + toIntervalHour(number % 360),
    (number % 10) + 1000, number % 50, number % 30,
    number % 50, number % 500
FROM numbers(1000);

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET max_threads = 4; -- override random max_threads=1 which makes 3-level nested view too slow
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- Plan check: outermost view (dv_dashboard) should be sent to parallel replicas.
SELECT '-- plan check: outermost view sent to replicas';
SELECT if(explain LIKE '%dv_dashboard%', 'dv_dashboard', if(explain LIKE '%v_dashboard%', 'v_dashboard', if(explain LIKE '%d_ssp%' OR explain LIKE '%d_dsp%', 'inner_view', 'other')))
FROM viewExplain('EXPLAIN', '', (
    SELECT
        sum(bids) AS Bids,
        sum(bid_requests) AS BidRequests,
        sum(impressions) AS Impressions,
        sum(clicks) AS Clicks,
        toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE
        hour >= toDateTime('2025-01-01 00:00:00', 'Europe/Paris')
        AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(internal_child_deal_id != 0, internal_child_deal_id, internal_deal_id) IN (200)
        AND network_id IN (3050)
    GROUP BY Day
    HAVING Impressions != 0 OR Bids != 0
    ORDER BY Day ASC
    SETTINGS parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadFromRemoteParallelReplicas%';

-- Pipeline check: all 4 base tables should use ReadPoolParallelReplicas in the local plan.
SELECT '-- pipeline check: all tables use ReadPoolParallelReplicas';
SELECT count()
FROM viewExplain('EXPLAIN PIPELINE', '', (
    SELECT
        sum(bids) AS Bids,
        sum(bid_requests) AS BidRequests,
        sum(impressions) AS Impressions,
        sum(clicks) AS Clicks,
        toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE
        hour >= toDateTime('2025-01-01 00:00:00', 'Europe/Paris')
        AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(internal_child_deal_id != 0, internal_child_deal_id, internal_deal_id) IN (200)
        AND network_id IN (3050)
    GROUP BY Day
    HAVING Impressions != 0 OR Bids != 0
    ORDER BY Day ASC
    SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_allow_view_over_mergetree = 1
))
WHERE explain LIKE '%ReadPoolParallelReplicas%';

-- Correctness: EXCEPT should produce zero rows.
SELECT '-- correctness';
(
    SELECT sum(bids) AS Bids, sum(bid_requests) AS BidRequests, sum(impressions) AS Impressions, sum(clicks) AS Clicks, toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE hour >= toDateTime('2025-01-01 00:00:00', 'Europe/Paris') AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(internal_child_deal_id != 0, internal_child_deal_id, internal_deal_id) IN (200) AND network_id IN (3050)
    GROUP BY Day HAVING Impressions != 0 OR Bids != 0
    ORDER BY ALL
    SETTINGS enable_parallel_replicas = 0
)
EXCEPT
(
    SELECT sum(bids) AS Bids, sum(bid_requests) AS BidRequests, sum(impressions) AS Impressions, sum(clicks) AS Clicks, toStartOfDay(hour, 'Europe/Paris') AS Day
    FROM dv_dashboard
    WHERE hour >= toDateTime('2025-01-01 00:00:00', 'Europe/Paris') AND hour < toDateTime('2026-03-01 00:00:00', 'Europe/Paris')
        AND if(internal_child_deal_id != 0, internal_child_deal_id, internal_deal_id) IN (200) AND network_id IN (3050)
    GROUP BY Day HAVING Impressions != 0 OR Bids != 0
    ORDER BY ALL
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_allow_view_over_mergetree = 1
);

DROP VIEW dv_dashboard;
DROP VIEW v_dashboard;
DROP VIEW d_dsp;
DROP VIEW d_ssp;
DROP TABLE t_dsp_current;
DROP TABLE t_dsp_archive;
DROP TABLE t_ssp_current;
DROP TABLE t_ssp_archive;
