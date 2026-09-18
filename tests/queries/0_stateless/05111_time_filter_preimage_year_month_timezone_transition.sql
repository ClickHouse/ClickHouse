-- `optimize_time_filter_with_preimage` rewrites `toYear(x) = y` into `x >= 'y-01-01 00:00:00' AND x < 'y+1-01-01 00:00:00'`,
-- with the literals re-parsed in the column's time zone. A year or month boundary is not always local midnight:
-- `America/Lima` started 1994 at 01:00:00 (the clocks jumped from 1993-12-31 23:59:59 to 1994-01-01 01:00:00), and
-- `America/Managua` started 1993 the same way. The preimage has to carry the actual local boundary, exactly as the
-- `toStartOfYear` / `toStartOfMonth` preimages already do, or the rows in the transition hour are misclassified.

SET session_timezone = 'UTC';
SET optimize_time_filter_with_preimage = 1;
-- The analyzer is not pinned session-wide on purpose: the result checks then cover the old AST
-- optimizer too on the CI run that turns the analyzer off. Only the `EXPLAIN QUERY TREE` checks,
-- which exist in the analyzer alone, pin it per query.

DROP TABLE IF EXISTS t_preimage_transition;
CREATE TABLE t_preimage_transition
(
    lima DateTime('America/Lima'),
    lima64 DateTime64(3, 'America/Lima'),
    managua DateTime('America/Managua'),
    managua64 DateTime64(6, 'America/Managua')
)
ENGINE = MergeTree ORDER BY tuple();

-- The last second of the old period, then one and two hours later: both land in the new period, after the 01:00:00 jump.
INSERT INTO t_preimage_transition SELECT
    toDateTime('1993-12-31 23:59:59', 'America/Lima') + number * 3600,
    toDateTime64('1993-12-31 23:59:59', 3, 'America/Lima') + number * 3600,
    toDateTime('1992-12-31 23:59:59', 'America/Managua') + number * 3600,
    toDateTime64('1992-12-31 23:59:59', 6, 'America/Managua') + number * 3600
FROM numbers(3);

SELECT lima, toYear(lima), lima64, toYYYYMM(lima64), managua, toYear(managua), managua64, toYYYYMM(managua64) FROM t_preimage_transition ORDER BY lima;

SELECT 'Lima toYear';
SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1993;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1994;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1994 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) != 1993;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) < 1994;
SELECT count() FROM t_preimage_transition WHERE toYear(lima) >= 1994;
SELECT count() FROM t_preimage_transition WHERE toYear(lima64) = 1993;
SELECT count() FROM t_preimage_transition WHERE toYear(lima64) = 1994;
SELECT count() FROM t_preimage_transition WHERE toYear(lima64) = 1994 SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Lima toYYYYMM';
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(lima) = 199312;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(lima) = 199401;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(lima) = 199401 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(lima64) = 199312;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(lima64) = 199401;

SELECT 'Managua toYear';
SELECT count() FROM t_preimage_transition WHERE toYear(managua) = 1992;
SELECT count() FROM t_preimage_transition WHERE toYear(managua) = 1993;
SELECT count() FROM t_preimage_transition WHERE toYear(managua) = 1993 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_transition WHERE toYear(managua64) = 1992;
SELECT count() FROM t_preimage_transition WHERE toYear(managua64) = 1993;

SELECT 'Managua toYYYYMM';
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua) = 199212;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua) = 199301;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua) = 199301 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua64) = 199212;
SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua64) = 199301;

SELECT 'the rewrite still applies and carries the real local boundary';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1993) WHERE explain LIKE '%1994-01-01 01:00:00%' SETTINGS enable_analyzer = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_preimage_transition WHERE toYYYYMM(managua64) = 199212) WHERE explain LIKE '%1993-01-01 01:00:00%' SETTINGS enable_analyzer = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_preimage_transition WHERE toYear(lima) = 1993) WHERE explain LIKE '%function_name: toYear%' SETTINGS enable_analyzer = 1;

SELECT 'an implicit time zone is parsed in the session time zone';
DROP TABLE IF EXISTS t_preimage_implicit;
CREATE TABLE t_preimage_implicit (dt DateTime, dt64 DateTime64(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_preimage_implicit VALUES ('2020-12-31 23:59:59', '2020-12-31 23:59:59'), ('2021-01-01 00:00:00', '2021-01-01 00:00:00');
SELECT count() FROM t_preimage_implicit WHERE toYear(dt) = 2020 SETTINGS session_timezone = 'Asia/Tokyo';
SELECT count() FROM t_preimage_implicit WHERE toYear(dt) = 2020 SETTINGS session_timezone = 'Asia/Tokyo', optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_implicit WHERE toYYYYMM(dt64) = 202101 SETTINGS session_timezone = 'America/New_York';
SELECT count() FROM t_preimage_implicit WHERE toYYYYMM(dt64) = 202101 SETTINGS session_timezone = 'America/New_York', optimize_time_filter_with_preimage = 0;

DROP TABLE t_preimage_transition;
DROP TABLE t_preimage_implicit;
