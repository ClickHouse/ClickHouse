-- https://github.com/ClickHouse/ClickHouse/issues/119557
-- `toString` over a `Nullable` key was declared monotonic whatever the wrapped type. Every cell prints 1;
-- the reference count comes from a `Log` copy, which has no primary key to consult.

-- `EXPLAIN indexes = 1` has no index section in a remote-only parallel replicas plan.
SET enable_parallel_replicas = 0;
SET allow_suspicious_low_cardinality_types = 1;
SET enable_time_time64_type = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_ref;
DROP TABLE IF EXISTS tab_no_null;
DROP TABLE IF EXISTS tab_no_null_ref;
DROP TABLE IF EXISTS tab_dt;
DROP TABLE IF EXISTS tab_dt_ref;
DROP TABLE IF EXISTS tab_lc;
DROP TABLE IF EXISTS tab_lc_ref;
DROP TABLE IF EXISTS tab_utc;
DROP TABLE IF EXISTS tab_utc_ref;
DROP TABLE IF EXISTS tab_utc_plain;
DROP TABLE IF EXISTS tab_time;
DROP TABLE IF EXISTS tab_time_ref;
DROP TABLE IF EXISTS tab_str;
DROP TABLE IF EXISTS tab_str_ref;
DROP TABLE IF EXISTS tab_str_no_null;
DROP TABLE IF EXISTS tab_uint;
DROP TABLE IF EXISTS tab_enum;

-- The name order ('a' < 'b') is the reverse of the value order (1 < 2).
CREATE TABLE tab (e Nullable(Enum8('b' = 1, 'a' = 2))) ENGINE = MergeTree ORDER BY e
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab VALUES (1), (2), (2), (2), (2), (2), (NULL);
CREATE TABLE tab_ref ENGINE = Log AS SELECT * FROM tab;

SELECT 'nullable_enum_equals', (SELECT count() FROM tab WHERE toString(e) = 'b')
    = (SELECT count() FROM tab_ref WHERE toString(e) = 'b');
SELECT 'nullable_enum_in', (SELECT count() FROM tab WHERE toString(e) IN ('b', 'zz'))
    = (SELECT count() FROM tab_ref WHERE toString(e) IN ('b', 'zz'));
-- `use_primary_key = 0` still builds the `KeyCondition` in strict mode, which consults the same monotonicity.
SELECT 'nullable_enum_no_primary_key', (SELECT count() FROM tab WHERE toString(e) = 'b' SETTINGS use_primary_key = 0)
    = (SELECT count() FROM tab_ref WHERE toString(e) = 'b');

-- `CAST(e AS String)` of a `NULL` row throws at row level, so this table has none.
CREATE TABLE tab_no_null (e Nullable(Enum8('b' = 1, 'a' = 2, 'd' = 3, 'c' = 4))) ENGINE = MergeTree ORDER BY e
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_no_null VALUES ('b'), ('a'), ('d'), ('c');
CREATE TABLE tab_no_null_ref ENGINE = Log AS SELECT * FROM tab_no_null;

SELECT 'nullable_enum_cast', (SELECT count() FROM tab_no_null WHERE CAST(e AS String) = 'a')
    = (SELECT count() FROM tab_no_null_ref WHERE CAST(e AS String) = 'a');

-- America/New_York turns the clocks back at 2021-11-07 06:00:00 UTC, so the local time 01:xx repeats.
CREATE TABLE tab_dt (d Nullable(DateTime('America/New_York'))) ENGINE = MergeTree ORDER BY d
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_dt VALUES
    (toDateTime('2021-11-07 05:30:00', 'UTC')), (toDateTime('2021-11-07 06:00:00', 'UTC')),
    (toDateTime('2021-11-07 06:30:00', 'UTC')), (toDateTime('2021-11-07 07:00:00', 'UTC')),
    (toDateTime('2021-11-07 07:30:00', 'UTC')), (toDateTime('2021-11-07 08:00:00', 'UTC')), (NULL);
CREATE TABLE tab_dt_ref ENGINE = Log AS SELECT * FROM tab_dt;

SELECT 'nullable_datetime_equals', (SELECT count() FROM tab_dt WHERE toString(d) = '2021-11-07 01:30:00')
    = (SELECT count() FROM tab_dt_ref WHERE toString(d) = '2021-11-07 01:30:00');

CREATE TABLE tab_lc (d LowCardinality(Nullable(DateTime('America/New_York')))) ENGINE = MergeTree ORDER BY d
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_lc SELECT d FROM tab_dt;
CREATE TABLE tab_lc_ref ENGINE = Log AS SELECT * FROM tab_lc;

SELECT 'lc_nullable_datetime_equals', (SELECT count() FROM tab_lc WHERE toString(d) = '2021-11-07 01:30:00')
    = (SELECT count() FROM tab_lc_ref WHERE toString(d) = '2021-11-07 01:30:00');

-- The constant time zone of `toString(d, tz)` formats the value, whatever the key's own zone: a UTC key
-- formatted in America/New_York repeats 01:xx, and a New York key formatted in UTC does not.
CREATE TABLE tab_utc (d Nullable(DateTime('UTC'))) ENGINE = MergeTree ORDER BY d
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_utc SELECT d FROM tab_dt;
CREATE TABLE tab_utc_ref ENGINE = Log AS SELECT * FROM tab_utc;
CREATE TABLE tab_utc_plain (d DateTime('UTC')) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 2;
INSERT INTO tab_utc_plain SELECT d FROM tab_dt WHERE d IS NOT NULL;

SELECT 'nullable_utc_formatted_in_new_york', (SELECT count() FROM tab_utc WHERE toString(d, 'America/New_York') = '2021-11-07 01:30:00')
    = (SELECT count() FROM tab_utc_ref WHERE toString(d, 'America/New_York') = '2021-11-07 01:30:00');
SELECT 'plain_utc_formatted_in_new_york', (SELECT count() FROM tab_utc_plain WHERE toString(d, 'America/New_York') = '2021-11-07 01:30:00')
    = (SELECT count() FROM tab_utc_ref WHERE toString(d, 'America/New_York') = '2021-11-07 01:30:00');
SELECT 'new_york_formatted_in_utc_prunes', (SELECT sum(granules_read) < sum(granules_total)
    FROM (SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) AS granules_read,
                 toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_total
          FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_dt WHERE toString(d, 'UTC') = '2021-11-07 06:00:00'
                SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)));

-- Hours take as many digits as needed, so '100:00:00' sorts before '99:00:00' as a string.
CREATE TABLE tab_time (t Nullable(Time)) ENGINE = MergeTree ORDER BY t
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_time VALUES ('99:00:00'), ('100:00:00'), ('101:00:00'), ('102:00:00'), (NULL);
CREATE TABLE tab_time_ref ENGINE = Log AS SELECT * FROM tab_time;

SELECT 'nullable_time_equals', (SELECT count() FROM tab_time WHERE toString(t) = '100:00:00')
    = (SELECT count() FROM tab_time_ref WHERE toString(t) = '100:00:00');

-- Where `toString` is monotonic on the wrapped type, a `Nullable` key is now pruned like a plain one. `CAST`
-- to `String` shares the verdict; it throws on the `NULL` mark while the index is analysed, as `CAST` to `Date`
-- does on a `Nullable(DateTime)` key today, so the error is raised, not hidden.
CREATE TABLE tab_str (s Nullable(String)) ENGINE = MergeTree ORDER BY s
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_str VALUES ('aa'), ('bb'), ('cc'), ('dd'), ('ee'), (NULL);
CREATE TABLE tab_str_ref ENGINE = Log AS SELECT * FROM tab_str;

SELECT 'nullable_string_range', (SELECT count() FROM tab_str WHERE toString(s) >= 'cc')
    = (SELECT count() FROM tab_str_ref WHERE toString(s) >= 'cc');
-- Only the granule ['cc', 'ee'] is read; the last one, ['ee', NULL], is pruned as for a plain key.
SELECT 'nullable_string_prunes', (SELECT sum(granules_read) = 1 AND sum(granules_total) = 3
    FROM (SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) AS granules_read,
                 toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_total
          FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_str WHERE toString(s) = 'dd'
                SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)));
SELECT count() FROM tab_str WHERE CAST(s AS String) = 'cc'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
-- Read in key order, `LIMIT` stops before the `NULL` row is reached, as `ORDER BY CAST(d AS Date)` on a
-- `Nullable(DateTime)` key does today; without the order the whole column is evaluated.
SELECT s FROM tab_str ORDER BY CAST(s AS String) LIMIT 1 SETTINGS optimize_read_in_order = 1;
SELECT s FROM tab_str ORDER BY CAST(s AS String) LIMIT 1 SETTINGS optimize_read_in_order = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
-- Without a `NULL` in the part, `CAST` prunes as `toString` does.
CREATE TABLE tab_str_no_null (s Nullable(String)) ENGINE = MergeTree ORDER BY s
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_str_no_null VALUES ('aa'), ('bb'), ('cc'), ('dd'), ('ee'), ('ff');

SELECT 'nullable_string_cast_prunes_without_null', (SELECT sum(granules_read) = 1 AND sum(granules_total) = 3
    FROM (SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) AS granules_read,
                 toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_total
          FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_str_no_null WHERE CAST(s AS String) = 'aa'
                SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)));

-- Controls: a monotonic `toString` still prunes a `Nullable` key, and a bare `Enum` still does not.
CREATE TABLE tab_uint (n Nullable(UInt64)) ENGINE = MergeTree ORDER BY n
    SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO tab_uint VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (NULL);

SELECT 'nullable_uint_prunes', (SELECT sum(granules_read) < sum(granules_total)
    FROM (SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) AS granules_read,
                 toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_total
          FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_uint WHERE toString(n) = '5'
                SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)));

CREATE TABLE tab_enum (e Enum8('b' = 1, 'a' = 2)) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 2;
INSERT INTO tab_enum VALUES (1), (2), (2), (2), (2), (2);

SELECT 'plain_enum_reads_all', (SELECT sum(granules_read) = sum(granules_total) AND sum(granules_total) > 0
    FROM (SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) AS granules_read,
                 toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_total
          FROM (EXPLAIN indexes = 1 SELECT count() FROM tab_enum WHERE toString(e) = 'b'
                SETTINGS use_skip_indexes = 0, optimize_use_implicit_projections = 0)));

DROP TABLE tab;
DROP TABLE tab_ref;
DROP TABLE tab_no_null;
DROP TABLE tab_no_null_ref;
DROP TABLE tab_dt;
DROP TABLE tab_dt_ref;
DROP TABLE tab_lc;
DROP TABLE tab_lc_ref;
DROP TABLE tab_utc;
DROP TABLE tab_utc_ref;
DROP TABLE tab_utc_plain;
DROP TABLE tab_time;
DROP TABLE tab_time_ref;
DROP TABLE tab_str;
DROP TABLE tab_str_ref;
DROP TABLE tab_str_no_null;
DROP TABLE tab_uint;
DROP TABLE tab_enum;
