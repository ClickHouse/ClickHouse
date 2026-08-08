SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04826;
CREATE TABLE logs_04826
(
    `_time` DateTime64(9, 'UTC'),
    `_msg` String,
    `num` UInt64,
    `snum` Int64,
    `dec` Decimal128(20)
) ENGINE = MergeTree ORDER BY _time;

-- 24 hourly rows across 2024-01-02, with values pinned to precision edge cases.
INSERT INTO logs_04826 SELECT
    toDateTime64('2024-01-02 00:00:00', 9, 'UTC') + INTERVAL number HOUR,
    'msg',
    9007199254740992 + (number % 2),
    -5 + toInt64(number % 2),
    toDecimal128('10.5', 20) + toDecimal128('0.00000000000000000001', 20) * (number % 3)
FROM numbers(24);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04826';
SET dialect = 'logsql';

-- rate() divides by the effective intersection of ALL top-level `_time` filters:
-- a self-contained window narrowed by a comparison filter counts one day of rows
-- over a one-day denominator, in either order.
_time:[2024-01-01Z, 2024-01-03Z) _time:>=2024-01-02Z | stats rate() as r | math round(r*86400) as per_day | fields per_day;
_time:>=2024-01-02Z _time:[2024-01-01Z, 2024-01-03Z) | stats rate() as r | math round(r*86400) as per_day | fields per_day;

-- Two self-contained windows intersect as well (the narrower one wins regardless of order).
_time:[2024-01-01Z, 2024-01-03Z) _time:[2024-01-02Z, 2024-01-04Z) | stats rate() as r | math round(r*86400) as per_day | fields per_day;
_time:[2024-01-02Z, 2024-01-04Z) _time:[2024-01-01Z, 2024-01-03Z) | stats rate() as r | math round(r*86400) as per_day | fields per_day;

-- A single-timestamp window (the whole day) works with a comparison filter too.
_time:2024-01-02Z _time:<2024-01-02T12Z | stats rate() as r | math round(r*86400) as per_day | fields per_day;

-- Numeric `stats by` buckets stay exact above 2^53: adjacent values keep separate buckets.
* | stats by (num:1) count() as c | sort by (num);

-- An integral bucket step keeps the floor semantics for negative values (bucket of -5 with step 10 is -10),
-- and an integral offset stays exact as well.
* | stats by (snum:10) count() as c | sort by (snum);
* | stats by (num:10 offset 3) count() as c | sort by (num);

-- Fractional steps keep the Float64 path.
* | stats by (snum:0.5) count() as c | sort by (snum);

-- `math` keeps 64-bit integer literals exact.
* | math 18446744073709551615 - 18446744073709551614 as one | fields one | limit 1;
* | math num - 9007199254740992 as delta | stats by (delta) count() as c | sort by (delta);

-- High-precision Decimal values compare exactly: a Float64 path would round
-- 10.50000000000000000001 to 10.5 and give the wrong result for all of these.
dec:>10.5 | stats count() as c;
dec:=10.5 | stats count() as c;
dec:=10.50000000000000000001 | stats count() as c;
dec:<=10.50000000000000000001 | stats count() as c;
dec:range(10.5, 10.50000000000000000002] | stats count() as c;
dec:range[10.5, 10.5] | stats count() as c;

-- `rename` overwrites an already-existing target column instead of duplicating it.
* | math num - 9007199254740992 as a, num - 9007199254740991 as b | rename a as b | stats by (b) count() as c | sort by (b);

DROP TABLE logs_04826;
