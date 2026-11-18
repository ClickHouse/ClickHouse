-- Table alias equal to the database name of another table expression in the same FROM.
-- https://github.com/ClickHouse/ClickHouse/issues/122239

SELECT count() FROM system.one AS system, system.one AS o;
SELECT count() FROM system.one AS o, system.one AS system;
SELECT o.dummy, count() FROM system.one AS system INNER JOIN system.one AS o ON system.dummy = o.dummy GROUP BY o.dummy;
SELECT count() FROM (SELECT 1 AS x) AS system, system.one;
SELECT count() FROM numbers(1) AS system, system.one;
SELECT count() FROM system.one AS system WHERE dummy IN (SELECT dummy FROM system.one);

DROP TABLE IF EXISTS intraday_sensitivities;
DROP TABLE IF EXISTS intraday_positions;
CREATE TABLE intraday_sensitivities (account_id UInt64, run_at DateTime, calculation_value Float64) ENGINE = Memory;
CREATE TABLE intraday_positions (account_id UInt64, run_at DateTime, portfolio String) ENGINE = Memory;
INSERT INTO intraday_sensitivities VALUES (1, '2026-01-01 00:00:00', 1.5);
INSERT INTO intraday_positions VALUES (1, '2026-01-01 00:00:00', 'p1');

SELECT positions.portfolio, sum({CLICKHOUSE_DATABASE:Identifier}.calculation_value)
FROM {CLICKHOUSE_DATABASE:Identifier}.intraday_sensitivities AS {CLICKHOUSE_DATABASE:Identifier}
INNER JOIN {CLICKHOUSE_DATABASE:Identifier}.intraday_positions AS positions
    ON positions.account_id = {CLICKHOUSE_DATABASE:Identifier}.account_id AND positions.run_at = {CLICKHOUSE_DATABASE:Identifier}.run_at
GROUP BY positions.portfolio;

DROP TABLE intraday_sensitivities;
DROP TABLE intraday_positions;
