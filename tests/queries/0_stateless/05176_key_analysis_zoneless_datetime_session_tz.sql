-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/119173 on plain MergeTree.
--
-- A `DateTime`/`DateTime64` whose type name carries no time zone binds one when the type object is
-- built, so a table's key expression holds the zone of its DDL, not of the query. Execution parses a
-- string literal for such a column in the session zone, while key analysis converted it with a CAST
-- into the frozen zone, so the pruner looked for the key value of a different instant and skipped the
-- part holding the matching row: no error and no log line.
--
-- `pruned` goes through key analysis; `honest` is a SELECT-list `countIf` over every row, which no
-- pruning can touch. The two must agree in every arm.

SET session_timezone = 'UTC';                -- freezes every key type below, whatever the server's zone
SET optimize_time_filter_with_preimage = 0;  -- the rewrite would rewrite the `countIf` oracle as well
SET explain_query_plan_default = 'legacy';   -- for the Condition/Parts/Granules assertions
SET parallel_replicas_local_plan = 1;        -- for explain with indexes and key condition values

DROP TABLE IF EXISTS 05176_day, 05176_day_nullable, 05176_hour, 05176_cast_date, 05176_explicit_zone, 05176_dynamic;

-- Rows 1 and 2 are in UTC day 20240102 and row 3 in 20240105, so there are two parts to prune between.
-- Read in Asia/Tokyo, '2024-01-03 05:00:00' names row 2, so the honest answer for that literal is 1.
CREATE TABLE 05176_day (dt DateTime64(6), id Int32) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY id;
CREATE TABLE 05176_day_nullable (dt Nullable(DateTime64(6)), id Int32) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY id SETTINGS allow_nullable_key = 1;
CREATE TABLE 05176_hour (dt DateTime64(6), id Int32) ENGINE = MergeTree PARTITION BY cityHash64(toHour(dt)) ORDER BY id;
CREATE TABLE 05176_cast_date (dt DateTime64(6), id Int32) ENGINE = MergeTree PARTITION BY dt::Date ORDER BY id;
CREATE TABLE 05176_explicit_zone (dt DateTime64(6, 'UTC'), id Int32) ENGINE = MergeTree PARTITION BY toYYYYMMDD(dt) ORDER BY id;

INSERT INTO 05176_day VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2), ('2024-01-05 12:00:00', 3);
INSERT INTO 05176_day_nullable VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2), ('2024-01-05 12:00:00', 3);
INSERT INTO 05176_hour VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2), ('2024-01-05 12:00:00', 3);
INSERT INTO 05176_cast_date VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2), ('2024-01-05 12:00:00', 3);
INSERT INTO 05176_explicit_zone VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2), ('2024-01-05 12:00:00', 3);

SELECT '-- the key type keeps the zone it was created with (otherwise every arm below is a tautology)';
SELECT timeZoneOf(dt) FROM 05176_day LIMIT 1 SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- monotonic chain over a zone-less key, equality and range';
SELECT (SELECT count() FROM 05176_day WHERE dt = '2024-01-03 05:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00') FROM 05176_day) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT (SELECT count() FROM 05176_day WHERE dt >= '2024-01-03 05:00:00' AND dt < '2024-01-03 06:00:00') AS pruned,
       (SELECT countIf(dt >= '2024-01-03 05:00:00' AND dt < '2024-01-03 06:00:00') FROM 05176_day) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- the same key through a Nullable wrapper';
SELECT (SELECT count() FROM 05176_day_nullable WHERE dt = '2024-01-03 05:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00') FROM 05176_day_nullable) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- a timezone-sensitive non-monotonic key: the pruner must look for the key value computed in';
SELECT '-- the frozen zone (hour 20 in UTC), because that is the zone the stored values were computed in';
SELECT (SELECT count() FROM 05176_hour WHERE dt = '2024-01-03 05:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00') FROM 05176_hour) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 05176_hour WHERE dt = '2024-01-03 05:00:00'
)
WHERE trim(explain) ilike 'condition: %'
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT cityHash64(toHour(toDateTime64('2024-01-02 20:00:00', 6, 'UTC'))) AS the_frozen_zone_key_value;

SELECT '';
SELECT '-- a single CAST as the whole key expression';
SELECT (SELECT count() FROM 05176_cast_date WHERE dt = '2024-01-03 05:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-03 05:00:00') FROM 05176_cast_date) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- pruning is still active: the matching literal reads one part of two, a non-matching one none';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 05176_day WHERE dt = '2024-01-03 05:00:00'
)
WHERE trim(explain) ilike 'condition: %' OR trim(explain) ilike 'parts: %'
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 05176_day WHERE dt = '2024-02-01 05:00:00'
)
WHERE trim(explain) ilike 'condition: %' OR trim(explain) ilike 'parts: %'
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- negative control: a type that names its zone keeps parsing in it, whatever the session says';
SELECT (SELECT count() FROM 05176_explicit_zone WHERE dt = '2024-01-02 20:00:00') AS pruned,
       (SELECT countIf(dt = '2024-01-02 20:00:00') FROM 05176_explicit_zone) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- negative control: a constant that is not text is not parsed, so it keeps its own zone';
SELECT (SELECT count() FROM 05176_day WHERE dt = toDateTime64('2024-01-02 20:00:00', 6, 'UTC')) AS pruned,
       (SELECT countIf(dt = toDateTime64('2024-01-02 20:00:00', 6, 'UTC')) FROM 05176_day) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';

SELECT '';
SELECT '-- negative control: a key whose CAST input is not a DateTime still takes the direct fast path,';
SELECT '-- which is what makes granule pruning possible for a round trip that cannot be done safely';
CREATE TABLE 05176_dynamic (d Dynamic, id Int32) ENGINE = MergeTree ORDER BY CAST(d AS String) SETTINGS index_granularity = 1;
INSERT INTO 05176_dynamic VALUES ('alpha', 1), ('beta', 2);
SELECT (SELECT count() FROM 05176_dynamic WHERE d = 'alpha') AS pruned,
       (SELECT countIf(d = 'alpha') FROM 05176_dynamic) AS honest
SETTINGS session_timezone = 'Asia/Tokyo';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 05176_dynamic WHERE d = 'alpha'
)
WHERE trim(explain) ilike 'granules: %'
SETTINGS session_timezone = 'Asia/Tokyo';

DROP TABLE 05176_day, 05176_day_nullable, 05176_hour, 05176_cast_date, 05176_explicit_zone, 05176_dynamic;
