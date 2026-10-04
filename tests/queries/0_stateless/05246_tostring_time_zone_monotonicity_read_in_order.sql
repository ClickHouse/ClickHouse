-- https://github.com/ClickHouse/ClickHouse/issues/122993
-- `toString(x, tz)` formats in `tz`; whether it keeps the order of `x` depends on `tz`, not on the zone of the type of `x`.
SET explain_query_plan_default = 'legacy';
SET optimize_read_in_order = 1;
SET optimize_injective_functions_in_group_by = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (x DateTime('UTC')) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab VALUES ('2025-11-02 05:30:00'), ('2025-11-02 06:00:00'), ('2025-11-02 06:30:00');

-- The clocks go back at 06:00 UTC, so the strings are not in the order of `x`.
SELECT toString(x, 'America/New_York') AS s FROM tab ORDER BY s;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN actions = 1 SELECT toString(x, 'America/New_York') AS s FROM tab ORDER BY s) WHERE explain LIKE '%ReadType%';
SELECT toString(x, 'America/New_York') AS s, count() FROM tab GROUP BY s ORDER BY s SETTINGS optimize_aggregation_in_order = 1;

-- A fixed offset keeps the order.
SELECT trimLeft(explain) FROM (EXPLAIN PLAN actions = 1 SELECT toString(x, 'UTC') AS s FROM tab ORDER BY s) WHERE explain LIKE '%ReadType%';
DROP TABLE tab;

-- The result is not ordered by the time zone argument.
CREATE TABLE tab (tz String) ENGINE = MergeTree ORDER BY tz;
INSERT INTO tab VALUES ('Asia/Tokyo'), ('Europe/London'), ('Pacific/Honolulu');
SELECT toString(toDateTime('2025-11-02 06:00:00', 'UTC'), tz) AS s FROM tab ORDER BY s;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN actions = 1 SELECT toString(toDateTime('2025-11-02 06:00:00', 'UTC'), tz) AS s FROM tab ORDER BY s) WHERE explain LIKE '%ReadType%';
SELECT toString(toDateTime('2025-11-02 06:00:00', 'UTC'), tz) AS s, count() FROM tab GROUP BY s ORDER BY s SETTINGS optimize_aggregation_in_order = 1;
DROP TABLE tab;
