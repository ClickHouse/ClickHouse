-- Related: https://github.com/ClickHouse/clickhouse-private/issues/55715 (Cluster C, distributed)
-- CI failure: nested-alias USING keys were lost in queries shipped to shards.

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;
-- R3/R5 rely on the `sum` rewrite that drops the aliased body from the projection;
-- this setting is randomized in CI, so pin it to keep those cases deterministic.
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

CREATE TABLE events
(
    event_date Date, platform String, advertising_id String, idfv String,
    event_name String, event_revenue_usd String
) ENGINE = MergeTree ORDER BY event_date;
INSERT INTO events VALUES ('2024-01-01', 'android', 'aid1', '', 'install', '0'), ('2024-01-01', 'android', 'aid1', '', 'af_purchase', '5.0'), ('2024-01-02', 'ios', '', 'idfv1', 'install', '0'), ('2024-01-02', 'ios', '', 'idfv1', 'af_purchase', '3.0');
CREATE TABLE t_shadow (x UInt64, id UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_shadow VALUES (1, 5);

-- R1: CI failure shape, left table via remote over two shards.
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM remote('127.0.0.{1,2}', currentDatabase(), events) AS iap
INNER JOIN (
    SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id, min(event_date) AS InstallDate
    FROM events WHERE event_name = 'install' GROUP BY id
) AS sub USING (id)
WHERE event_name LIKE 'af_%' AND toFloat64OrZero(event_revenue_usd) > 0;

-- R2: nested alias shadowing a real column, body survives optimization (uniqExact is not rewritten).
SELECT uniqExact(x + 10 AS id) FROM remote('127.0.0.{1,2}', currentDatabase(), t_shadow) AS tsh JOIN (SELECT 11 AS id) t2 USING (id);

-- R3: fail-close: shadowing where an optimization rewrites the body out of the projection
-- (sum(x + 10) becomes sum(x) + 10 * count()), so the alias cannot be restored; throws on the initiator
-- instead of silently joining by the shadowed column.
SELECT sum(x + 10 AS id) FROM remote('127.0.0.{1,2}', currentDatabase(), t_shadow) AS tsh JOIN (SELECT 11 AS id) t2 USING (id); -- { serverError UNSUPPORTED_METHOD }

-- R4: join_use_nulls.
SELECT uniqExact(lower(if(platform = 'android', advertising_id, idfv)) AS id) AS users_pay
FROM remote('127.0.0.{1,2}', currentDatabase(), events) AS iap
LEFT JOIN (SELECT lower(if(platform = 'ios', idfv, advertising_id)) AS id FROM events WHERE event_name = 'install' GROUP BY id) AS sub USING (id)
WHERE event_name LIKE 'af_%'
SETTINGS join_use_nulls = 1;

-- R5: non-shadowing key whose body was rewritten out of the projection stays a loud shard-side error.
SELECT sum(x + 10 AS newid) FROM remote('127.0.0.{1,2}', currentDatabase(), t_shadow) AS tsh JOIN (SELECT 11 AS newid) t2 USING (newid); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE events;
DROP TABLE t_shadow;
