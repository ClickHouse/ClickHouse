SET max_threads = 2;
SET optimize_merge_neutral_sum_children = 1;
SET optimize_use_projections = 1;
SET optimize_distinct_in_order = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS merge_neutral_sum;
DROP TABLE IF EXISTS merge_neutral_sum_child_pnl;
DROP TABLE IF EXISTS merge_neutral_sum_child_no_pnl;
DROP TABLE IF EXISTS merge_neutral_sum_high;
DROP TABLE IF EXISTS merge_neutral_sum_high_pnl;
DROP TABLE IF EXISTS merge_neutral_sum_high_no_pnl;

CREATE TABLE merge_neutral_sum_child_pnl
(
    cob Date,
    desk LowCardinality(String),
    trader LowCardinality(String),
    pnl Nullable(Float64)
) ENGINE = MergeTree ORDER BY cob SETTINGS index_granularity = 2048;

CREATE TABLE merge_neutral_sum_child_no_pnl
(
    cob Date,
    desk LowCardinality(String),
    trader LowCardinality(String),
    delta Float64,
    PROJECTION group_keys (SELECT cob, desk, trader, sum(delta) GROUP BY cob, desk, trader)
) ENGINE = MergeTree ORDER BY cob SETTINGS index_granularity = 2048;

INSERT INTO merge_neutral_sum_child_pnl VALUES
    ('2026-01-01', 'desk_a', 'trader_a', 10),
    ('2026-01-01', 'desk_a', 'trader_a', 20),
    ('2026-01-01', 'desk_b', 'trader_b', NULL),
    ('2026-01-02', 'desk_a', 'trader_a', 7),
    ('2026-01-03', 'desk_c', 'trader_c', 4);
INSERT INTO merge_neutral_sum_child_no_pnl
SELECT toDate('2026-01-01') + number % 3, toString(number % 10), toString(number % 11), 1
FROM numbers(1000000);
ALTER TABLE merge_neutral_sum_child_no_pnl MATERIALIZE PROJECTION group_keys;

CREATE TABLE merge_neutral_sum AS merge_neutral_sum_child_pnl
ENGINE = Merge(currentDatabase(), '^merge_neutral_sum_child_(pnl|no_pnl)$');

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    GROUP BY cob, desk, trader
)
SETTINGS optimize_merge_neutral_sum_children = 0, log_comment = '05238_neutral_sum_off';

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum';

SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x, position(x, '.') + 1), projections))
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '05238_neutral_sum'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    WHERE cob = '2026-01-01'
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_where';

SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x, position(x, '.') + 1), projections))
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '05238_neutral_sum_where'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    WHERE cob IN ('2026-01-01', '2026-01-03') AND desk IN ('desk_a', 'desk_c')
    GROUP BY cob, desk, trader
)
SETTINGS optimize_merge_neutral_sum_children = 0, log_comment = '05238_neutral_sum_in_off';

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    WHERE cob IN ('2026-01-01', '2026-01-03') AND desk IN ('desk_a', 'desk_c')
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_in';

SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x, position(x, '.') + 1), projections))
FROM system.user_query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '05238_neutral_sum_in'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    PREWHERE cob = '2026-01-02'
    GROUP BY cob, desk, trader
)
SETTINGS optimize_merge_neutral_sum_children = 1, log_comment = '05238_neutral_sum_prewhere';

SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum
    WHERE pnl > 0
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_measure_filter';

-- A neutral child without the grouping-key projection must remain a raw scan.
CREATE TABLE merge_neutral_sum_child_no_projection
(
    cob Date,
    desk LowCardinality(String),
    trader LowCardinality(String),
    delta Float64
) ENGINE = MergeTree ORDER BY cob;
INSERT INTO merge_neutral_sum_child_no_projection VALUES ('2026-01-01', 'raw_only', 'trader', 1);
CREATE TABLE merge_neutral_sum_no_projection AS merge_neutral_sum_child_pnl
ENGINE = Merge(currentDatabase(), '^merge_neutral_sum_child_(pnl|no_projection)$');
SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum_no_projection
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_no_projection';

-- A Merge-level non-NULL default invalidates the typed-NULL proof and must decline.
CREATE TABLE merge_neutral_sum_with_default
(
    cob Date,
    desk LowCardinality(String),
    trader LowCardinality(String),
    pnl Nullable(Float64) DEFAULT 7
) ENGINE = Merge(currentDatabase(), '^merge_neutral_sum_child_(pnl|no_projection)$');
SELECT count(), sum(ifNull(pnl_sum, 0)), countIf(isNull(pnl_sum))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum_with_default
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_default';

-- High-cardinality fallback: the projection has one row per base row, so the
-- cost gate must leave the original child plan and preserve all groups.
CREATE TABLE merge_neutral_sum_high_pnl
(
    cob Date,
    desk String,
    trader String,
    pnl Nullable(Float64)
) ENGINE = MergeTree ORDER BY cob;
CREATE TABLE merge_neutral_sum_high_no_pnl
(
    cob Date,
    desk String,
    trader String,
    delta Float64,
    PROJECTION group_keys (SELECT cob, desk, trader, sum(delta) GROUP BY cob, desk, trader)
) ENGINE = MergeTree ORDER BY cob;
INSERT INTO merge_neutral_sum_high_pnl VALUES ('2026-01-01', 'pnl', 'trader', 10);
INSERT INTO merge_neutral_sum_high_no_pnl
SELECT toDate('2026-01-01') + number % 1000, toString(number), toString(number), 1
FROM numbers(20000);
ALTER TABLE merge_neutral_sum_high_no_pnl MATERIALIZE PROJECTION group_keys;
CREATE TABLE merge_neutral_sum_high AS merge_neutral_sum_high_pnl
ENGINE = Merge(currentDatabase(), '^merge_neutral_sum_high_(pnl|no_pnl)$');
SELECT count(), sum(ifNull(pnl_sum, 0))
FROM
(
    SELECT cob, desk, trader, sum(pnl) AS pnl_sum
    FROM merge_neutral_sum_high
    GROUP BY cob, desk, trader
)
SETTINGS log_comment = '05238_neutral_sum_high';

DROP TABLE merge_neutral_sum;
DROP TABLE merge_neutral_sum_no_projection;
DROP TABLE merge_neutral_sum_with_default;
DROP TABLE merge_neutral_sum_child_no_projection;
DROP TABLE merge_neutral_sum_child_no_pnl;
DROP TABLE merge_neutral_sum_child_pnl;
DROP TABLE merge_neutral_sum_high;
DROP TABLE merge_neutral_sum_high_no_pnl;
DROP TABLE merge_neutral_sum_high_pnl;
