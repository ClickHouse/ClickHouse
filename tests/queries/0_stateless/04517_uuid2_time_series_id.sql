-- Tags: no-ordinary-database, no-replicated-database
-- The test inspects the Atomic inner-table names (`.inner_id.tags.<uuid>`) of a TimeSeries table and reads
-- the generated `id` via `merge`, so it requires an Atomic, non-Replicated database.

-- Regression test for the UUID2 data type as a TimeSeries `id` column, and for `uuid_type_version = 2`
-- consistently materializing a bare `UUID` id to `UUID2` across the samples and tags inner tables.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SET allow_experimental_time_series_table = 1;

-- Each query below reports the distinct `id` column type across the samples and tags inner tables of one
-- TimeSeries table. A single value proves both inner tables agree on the `id` type. The table is dropped right
-- after its check so the final INSERT test can use `merge` over the only remaining inner tables.

-- 1. version = 2: a plain TimeSeries table materializes the `UUID` inside the default `Tuple(UInt64, UUID)` id
--    to `UUID2` consistently.
SET uuid_type_version = 2;
CREATE TABLE ts_v2_default ENGINE = TimeSeries;
SELECT 'v2_default', arraySort(groupUniqArray(type)) FROM system.columns
WHERE database = currentDatabase() AND name = 'id'
  AND table LIKE '.inner_id.%.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v2_default');
DROP TABLE ts_v2_default;

-- 2. version = 2: an explicit bare `UUID` id declared in TAGS INNER COLUMNS is also materialized to `UUID2`.
CREATE TABLE ts_v2_tags_uuid ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID);
SELECT 'v2_tags_uuid', arraySort(groupUniqArray(type)) FROM system.columns
WHERE database = currentDatabase() AND name = 'id'
  AND table LIKE '.inner_id.%.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v2_tags_uuid');
DROP TABLE ts_v2_tags_uuid;

-- 3. version = 2: a non-UUID id (UInt64) is left untouched.
CREATE TABLE ts_v2_uint64 ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags));
SELECT 'v2_uint64', arraySort(groupUniqArray(type)) FROM system.columns
WHERE database = currentDatabase() AND name = 'id'
  AND table LIKE '.inner_id.%.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v2_uint64');
DROP TABLE ts_v2_uint64;

-- 4. An explicit `UUID2` id works regardless of the setting.
SET uuid_type_version = 1;
CREATE TABLE ts_explicit_uuid2 ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID2);
SELECT 'explicit_uuid2', arraySort(groupUniqArray(type)) FROM system.columns
WHERE database = currentDatabase() AND name = 'id'
  AND table LIKE '.inner_id.%.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_explicit_uuid2');
DROP TABLE ts_explicit_uuid2;

-- 5. version = 1: a bare `UUID` id stays `UUID` (the setting does not change existing behavior).
CREATE TABLE ts_v1_uuid ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID);
SELECT 'v1_uuid', arraySort(groupUniqArray(type)) FROM system.columns
WHERE database = currentDatabase() AND name = 'id'
  AND table LIKE '.inner_id.%.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v1_uuid');
DROP TABLE ts_v1_uuid;

-- 6. The auto-generated `id` with a `UUID2` component is a valid, non-zero identifier.
SET uuid_type_version = 2;
CREATE TABLE ts_insert ENGINE = TimeSeries;
INSERT INTO ts_insert (metric_name, tags, time_series) VALUES ('http_requests', map('job', 'api'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.0)]);
SELECT 'insert', toTypeName(id), tupleElement(id, 2) != toUUID2('00000000-0000-0000-0000-000000000000'), count()
FROM merge(currentDatabase(), '^\.inner_id\.tags\.')
GROUP BY id;
DROP TABLE ts_insert;
