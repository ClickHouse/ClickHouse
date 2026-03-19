-- Test: TTL GROUP BY expression keys and ORDER BY expressions are handled with vertical insert.
DROP TABLE IF EXISTS t_vi_ttl_group_expr;

CREATE TABLE t_vi_ttl_group_expr
(
    ts DateTime,
    id String,
    value UInt32
)
ENGINE = MergeTree
ORDER BY (id, toStartOfDay(ts), toStartOfHour(ts))
TTL ts + toIntervalDay(1)
    GROUP BY id, toStartOfDay(ts)
    SET value = max(value)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

WITH toDateTime(today()) AS base
INSERT INTO t_vi_ttl_group_expr
SELECT base + INTERVAL 1 HOUR, 'a', 1
UNION ALL
SELECT base + INTERVAL 2 HOUR, 'a', 2
UNION ALL
SELECT base + INTERVAL 26 HOUR, 'a', 3;

SELECT
    id,
    dateDiff('day', today(), toDate(toStartOfDay(ts))) AS d,
    max(value) AS v
FROM t_vi_ttl_group_expr
GROUP BY id, d
ORDER BY id, d;

DROP TABLE t_vi_ttl_group_expr;
