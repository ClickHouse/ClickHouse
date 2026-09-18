SET session_timezone = 'UTC';
-- For explain with indexes and key condition values verification
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS 03636_data_pk, 03636_data_partitions, 03636_data_parsed;

CREATE TABLE 03636_data_pk (ts DateTime) ENGINE = MergeTree ORDER BY toStartOfDay(ts)
AS
SELECT 1756882680;

SELECT '-- PK UTC timezone';

SELECT count() FROM 03636_data_pk WHERE ts = 1756882680;

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_pk WHERE ts = 1756882680
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %';

SELECT '';
SELECT '-- PK EST timezone';

SELECT count() FROM 03636_data_pk WHERE ts = 1756882680 SETTINGS session_timezone = 'EST';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_pk WHERE ts = 1756882680
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %'
SETTINGS session_timezone = 'EST';

DROP TABLE 03636_data_pk;

CREATE TABLE 03636_data_partitions (ts DateTime) ENGINE = MergeTree ORDER BY tuple() PARTITION BY toStartOfDay(ts)
AS
SELECT 1756882680;

SELECT '';
SELECT '-- Partitions UTC timezone';

SELECT count() FROM 03636_data_partitions WHERE ts = 1756882680;

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_partitions WHERE ts = 1756882680
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %';

SELECT '';
SELECT '-- Partitions EST timezone';

SELECT count() FROM 03636_data_partitions WHERE ts = 1756882680 SETTINGS session_timezone = 'EST';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_partitions WHERE ts = 1756882680
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %'
SETTINGS session_timezone = 'EST';

DROP TABLE 03636_data_partitions;

CREATE TABLE 03636_data_parsed (ts String) ENGINE = MergeTree ORDER BY toStartOfDay(toDateTime(ts))
AS
SELECT '2025-09-02 19:00:00';

SELECT '';
SELECT '-- Partitions UTC timezone';

SELECT count() FROM 03636_data_parsed WHERE ts = '2025-09-02 19:00:00';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_parsed WHERE ts = '2025-09-02 19:00:00'
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %';

SELECT '';
SELECT '-- Partitions EST timezone';

SELECT count() FROM 03636_data_parsed WHERE ts = '2025-09-02 19:00:00' SETTINGS session_timezone = 'EST';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT count() FROM 03636_data_parsed WHERE ts = '2025-09-02 19:00:00'
)
WHERE trim(explain) ilike 'condition: %'
   OR trim(explain) ilike 'parts: %'
   OR trim(explain) ilike 'granules: %'
SETTINGS session_timezone = 'EST';

DROP TABLE 03636_data_parsed;
