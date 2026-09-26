-- Tags: no-random-settings

-- Negative test for LIMIT push-down into aggregation-in-order.
-- A row-count-reducing FilterStep sits between the aggregation and the outer
-- LIMIT: SELECT ... FROM (... GROUP BY key) WHERE c > N ORDER BY key LIMIT k.
-- The optimization must NOT push the limit past the FilterStep, otherwise the
-- aggregator would stop after producing k *unfiltered* groups, and fewer than k
-- rows would survive the filter — a wrong, truncated result.
--
-- We assert that the result is identical with the optimization enabled and
-- disabled. If the limit were wrongly pushed past the filter the two would
-- diverge.

DROP TABLE IF EXISTS t_agg_in_order_limit_filter;

CREATE TABLE t_agg_in_order_limit_filter (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16;

-- 100 distinct keys (0..99). Key k has (k % 10 + 1) rows, so count() per group
-- ranges over 1..10 and the WHERE c > 5 filter removes a large, interleaved set
-- of leading groups — exactly the groups a wrongly-pushed limit would keep.
INSERT INTO t_agg_in_order_limit_filter
SELECT key, value
FROM (
    SELECT key, arrayJoin(range((key % 10) + 1)) AS value
    FROM (SELECT number AS key FROM numbers(100))
)
ORDER BY key;

-- Filter between aggregation and limit. Result must match with push-down on/off.
SELECT
(
    SELECT groupArray((key, c))
    FROM
    (
        SELECT key, count() AS c FROM t_agg_in_order_limit_filter GROUP BY key
        HAVING c > 5
        ORDER BY key ASC
        LIMIT 5
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
    )
)
=
(
    SELECT groupArray((key, c))
    FROM
    (
        SELECT key, count() AS c FROM t_agg_in_order_limit_filter GROUP BY key
        HAVING c > 5
        ORDER BY key ASC
        LIMIT 5
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0
    )
) AS having_results_match;

-- Same, but with the filter applied in an outer subquery (a real FilterStep
-- between the aggregation and the sort/limit, not a HAVING fused into the
-- aggregation), and an OFFSET to shift the surviving window.
SELECT
(
    SELECT groupArray((key, c))
    FROM
    (
        SELECT key, c
        FROM (SELECT key, count() AS c FROM t_agg_in_order_limit_filter GROUP BY key)
        WHERE c > 5
        ORDER BY key ASC
        LIMIT 7 OFFSET 3
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
    )
)
=
(
    SELECT groupArray((key, c))
    FROM
    (
        SELECT key, c
        FROM (SELECT key, count() AS c FROM t_agg_in_order_limit_filter GROUP BY key)
        WHERE c > 5
        ORDER BY key ASC
        LIMIT 7 OFFSET 3
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0
    )
) AS filter_results_match;

-- Pin down the actual (filtered) rows so the reference is not vacuously satisfied.
SELECT key, count() AS c
FROM t_agg_in_order_limit_filter
GROUP BY key
HAVING c > 5
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1;

DROP TABLE t_agg_in_order_limit_filter;
