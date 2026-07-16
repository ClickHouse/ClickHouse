-- Related: https://github.com/ClickHouse/clickhouse-private/issues/55715 (Cluster C)
-- Resolving JOIN USING identifiers from aliases nested inside SELECT-list
-- expressions under analyzer_compatibility_join_using_top_level_identifier = 1.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS events;

CREATE TABLE events
(
    event_date Date, platform String, advertising_id String, idfv String,
    event_name String, event_revenue_usd String
) ENGINE = MergeTree ORDER BY event_date;
INSERT INTO events VALUES ('2024-01-01', 'android', 'aid1', '', 'install', '0'), ('2024-01-01', 'android', 'aid1', '', 'af_purchase', '5.0'), ('2024-01-02', 'ios', '', 'idfv1', 'install', '0'), ('2024-01-02', 'ios', '', 'idfv1', 'af_purchase', '3.0');

-- case 1: Cluster C production repro. Default (setting = 0) cannot resolve the alias.
SET analyzer_compatibility_join_using_top_level_identifier = 0;
WITH dateDiff('day', InstallDate, event_date) AS lifetime
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM events AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0; -- { serverError UNKNOWN_IDENTIFIER }

SET analyzer_compatibility_join_using_top_level_identifier = 1;
WITH dateDiff('day', InstallDate, event_date) AS lifetime
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM events AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0;

DROP TABLE events;

-- case 2: minimal nested-in-aggregate alias.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT sum(x + 1 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SET analyzer_compatibility_join_using_top_level_identifier = 0;
SELECT sum(x + 1 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }

-- case 3: alias nested in a plain function.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT concat('v', toString(x + 1 AS id)) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id);

-- case 4: nested alias takes priority over a real left column (old-analyzer-compatible).
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT sum(x + 10 AS id) FROM (SELECT 1 AS x, 2 AS id) t1 JOIN (SELECT 2 AS id) t2 USING (id);
SET analyzer_compatibility_join_using_top_level_identifier = 0;
SELECT sum(x + 10 AS id) FROM (SELECT 1 AS x, 2 AS id) t1 JOIN (SELECT 2 AS id) t2 USING (id);

-- case 5: alias defined only in WHERE is out of scope.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT sum(x) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id) WHERE (x + 1 AS id) > 0; -- { serverError UNKNOWN_IDENTIFIER }

-- case 6: nested alias expression references a column absent from the left table.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT sum(y + 1 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id, 3 AS y) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }

-- case 7: duplicated nested alias with different expressions must not be picked arbitrarily.
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SELECT sum(x + 1 AS id) + sum(x + 2 AS id) FROM (SELECT 1 AS x) t1 JOIN (SELECT 2 AS id) t2 USING (id); -- { serverError UNKNOWN_IDENTIFIER }
