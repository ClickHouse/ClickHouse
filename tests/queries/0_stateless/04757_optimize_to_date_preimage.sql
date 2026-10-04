-- Scope: positive preimage coverage is limited to one-argument functions on `DateTime` columns;
-- the effective timezone comes from the column type. `DateTime64` sources and explicit-timezone
-- function overloads require result- or argument-aware bounds and are outside this iteration.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_optimize_to_date_preimage;
CREATE TABLE t_optimize_to_date_preimage
(
    ts DateTime('America/Los_Angeles'),
    INDEX idx_ts ts TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

-- America/Los_Angeles enters DST on 2026-03-08: 01:59:59 PST (09:59:59 UTC) is
-- immediately followed by 03:00:00 PDT (10:00:00 UTC), so this civil day is 23 hours long.
INSERT INTO t_optimize_to_date_preimage VALUES
    ('2025-12-28 23:59:59'),
    ('2025-12-29 00:00:00'),
    ('2026-02-28 23:59:59'),
    ('2026-03-07 23:59:59'),
    ('2026-03-08 00:00:00'),
    ('2026-03-08 01:59:59'),
    ('2026-03-08 03:00:00'),
    ('2026-03-08 23:59:59'),
    ('2026-03-09 00:00:00'),
    ('2026-04-01 00:00:00'),
    ('2027-01-03 23:59:59'),
    ('2027-01-04 00:00:00');

SELECT 'not optimized', arraySort(groupArray(toUnixTimestamp(ts)))
FROM t_optimize_to_date_preimage
WHERE toDate(ts) = toDate('2026-03-08')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'optimized', arraySort(groupArray(toUnixTimestamp(ts)))
FROM t_optimize_to_date_preimage
WHERE toDate(ts) = toDate('2026-03-08')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'query tree retains toDate', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toDate(ts) = toDate('2026-03-08')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toDate%';

SELECT 'today query tree retains toDate', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toDate(ts) = today()
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toDate%';

SELECT 'index condition retains toDate', count()
FROM
(
    EXPLAIN indexes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toDate(ts) = toDate('2026-03-08')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%toDate(ts)%';

SELECT 'month not optimized', count()
FROM t_optimize_to_date_preimage
WHERE toStartOfMonth(ts) = toDate('2026-03-01')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'month optimized', count()
FROM t_optimize_to_date_preimage
WHERE toStartOfMonth(ts) = toDate('2026-03-01')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'day not optimized', count()
FROM t_optimize_to_date_preimage
WHERE toStartOfDay(ts) = toDateTime('2026-03-08 00:00:00', 'America/Los_Angeles')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'day optimized', count()
FROM t_optimize_to_date_preimage
WHERE toStartOfDay(ts) = toDateTime('2026-03-08 00:00:00', 'America/Los_Angeles')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'last day of month not optimized', count()
FROM t_optimize_to_date_preimage
WHERE toLastDayOfMonth(ts) = toDate('2026-03-31')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'last day of month optimized', count()
FROM t_optimize_to_date_preimage
WHERE toLastDayOfMonth(ts) = toDate('2026-03-31')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'ISO year number not optimized', count()
FROM t_optimize_to_date_preimage
WHERE toISOYear(ts) = 2026
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'ISO year number optimized', count()
FROM t_optimize_to_date_preimage
WHERE toISOYear(ts) = 2026
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'YYYYMMDD not optimized', count()
FROM t_optimize_to_date_preimage
WHERE toYYYYMMDD(ts) = 20260308
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'YYYYMMDD optimized', count()
FROM t_optimize_to_date_preimage
WHERE toYYYYMMDD(ts) = 20260308
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'month query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfMonth(ts) = toStartOfMonth(toDate('2026-03-08'))
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfMonth%';

SELECT 'day query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfDay(ts) = toDateTime('2026-03-08 00:00:00', 'America/Los_Angeles')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfDay%';

SELECT 'week query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toMonday(ts) = toDate('2026-03-02')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toMonday%';

SELECT 'quarter query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfQuarter(ts) = toDate('2026-01-01')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfQuarter%';

SELECT 'year query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfYear(ts) = toDate('2026-01-01')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfYear%';

SELECT 'ISO year query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfISOYear(ts) = toDate('2025-12-29')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfISOYear%';

SELECT 'last day of month query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toLastDayOfMonth(ts) = toDate('2026-03-31')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toLastDayOfMonth%';

SELECT 'ISO year number query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toISOYear(ts) = 2026
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toISOYear%';

SELECT 'YYYYMMDD query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toYYYYMMDD(ts) = 20260308
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toYYYYMMDD%';

SELECT 'invalid last day query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toLastDayOfMonth(ts) = toDate('2026-03-30')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toLastDayOfMonth%';

SELECT 'invalid YYYYMMDD query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toYYYYMMDD(ts) = 20260230
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toYYYYMMDD%';

SELECT 'month index condition retains function', count()
FROM
(
    EXPLAIN indexes = 1
    SELECT ts
    FROM t_optimize_to_date_preimage
    WHERE toStartOfMonth(ts) = toStartOfMonth(toDate('2026-03-08'))
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%toStartOfMonth(ts)%';

DROP TABLE t_optimize_to_date_preimage;

-- The original preimage regression affected servers using America/Lima and America/Managua:
-- * https://github.com/ClickHouse/ClickHouse/pull/51795#issuecomment-1621286435
-- * https://github.com/ClickHouse/ClickHouse/pull/51795
--
-- America/Managua changes its offset at 1993-01-01 00:00:00, and America/Lima advances
-- at 1994-01-01 00:00:00. Keep both source zones explicit here to ensure a preimage bound
-- is represented by its actual local transition time and round-trips through a string constant.
DROP TABLE IF EXISTS t_optimize_timezone_preimage;
CREATE TABLE t_optimize_timezone_preimage
(
    lima DateTime('America/Lima'),
    managua DateTime('America/Managua')
)
ENGINE = Memory;

INSERT INTO t_optimize_timezone_preimage VALUES
    ('1992-12-31 23:59:59', '1992-12-31 23:59:59'),
    ('1993-01-01 00:00:00', '1993-01-01 00:00:00'),
    ('1993-12-31 23:59:59', '1993-12-31 23:59:59'),
    ('1994-01-01 00:00:00', '1994-01-01 00:00:00');

SELECT 'Lima not optimized', count()
FROM t_optimize_timezone_preimage
WHERE toStartOfYear(lima) = toDate('1993-01-01')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Lima optimized', count()
FROM t_optimize_timezone_preimage
WHERE toStartOfYear(lima) = toDate('1993-01-01')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'Lima query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT lima
    FROM t_optimize_timezone_preimage
    WHERE toStartOfYear(lima) = toDate('1993-01-01')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfYear%';

SELECT 'Managua not optimized', count()
FROM t_optimize_timezone_preimage
WHERE toStartOfYear(managua) = toDate('1993-01-01')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Managua optimized', count()
FROM t_optimize_timezone_preimage
WHERE toStartOfYear(managua) = toDate('1993-01-01')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'Managua query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT managua
    FROM t_optimize_timezone_preimage
    WHERE toStartOfYear(managua) = toDate('1993-01-01')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfYear%';

DROP TABLE t_optimize_timezone_preimage;

DROP TABLE IF EXISTS t_optimize_to_date64_preimage;
CREATE TABLE t_optimize_to_date64_preimage (ts DateTime64(3, 'UTC')) ENGINE = Memory;

SELECT 'DateTime64 retains toDate', count() > 0
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT ts
    FROM t_optimize_to_date64_preimage
    WHERE toDate(ts) = toDate('2026-03-08')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toDate%';

DROP TABLE t_optimize_to_date64_preimage;
