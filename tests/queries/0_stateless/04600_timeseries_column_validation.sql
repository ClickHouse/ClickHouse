-- Tags: no-replicated-database
-- Reason: the DETACH/ATTACH round-trip below hangs in DatabaseReplicated mode because ATTACH TABLE
-- with a TimeSeries engine goes through the replicated DDL log and requires replica sync (same as
-- 04146_timeseries_attach_detach.sql).

-- Test: exercises `TimeSeries` column-type validation during table creation.
-- Covers `normalizeTimeSeriesDefinition`:
--   - the prealpha columns `id`, `timestamp`, `value` are rejected from the outer column list (INCORRECT_QUERY);
--   - the `time_series` column must have type `Array(Tuple(timestamp, value))` (BAD_TYPE_OF_FIELD);
--   - the timestamp and value element types are validated (BAD_TYPE_OF_FIELD);
--   - the documented `time_series` type override is honored;
--   - extra outer metadata columns (`metric_name`, `tags`, ...) are accepted and normalized to the canonical
--     schema, which is what lets a normalized table round-trip through DETACH/ATTACH (and replication/backup).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_reserved_id;
DROP TABLE IF EXISTS ts_reserved_timestamp;
DROP TABLE IF EXISTS ts_reserved_value;
DROP TABLE IF EXISTS ts_bad_shape;
DROP TABLE IF EXISTS ts_bad_timestamp_type;
DROP TABLE IF EXISTS ts_bad_value_type;
DROP TABLE IF EXISTS ts_valid;
DROP TABLE IF EXISTS ts_override;
DROP TABLE IF EXISTS ts_extra_outer;

-- The prealpha columns `id`, `timestamp`, `value` are not allowed in the outer column list.
CREATE TABLE ts_reserved_id (id String) ENGINE = TimeSeries; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_reserved_timestamp (timestamp String) ENGINE = TimeSeries; -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_reserved_value (value Int32) ENGINE = TimeSeries; -- { serverError INCORRECT_QUERY }

-- The `time_series` column must have type Array(Tuple(timestamp, value)), not a scalar type.
CREATE TABLE ts_bad_shape (time_series String) ENGINE = TimeSeries; -- { serverError BAD_TYPE_OF_FIELD }

-- The timestamp element must be a date/time type, not String.
CREATE TABLE ts_bad_timestamp_type (time_series Array(Tuple(String, Float64))) ENGINE = TimeSeries; -- { serverError BAD_TYPE_OF_FIELD }

-- The value element must be a floating-point type, not String.
CREATE TABLE ts_bad_value_type (time_series Array(Tuple(DateTime64(3), String))) ENGINE = TimeSeries; -- { serverError BAD_TYPE_OF_FIELD }

-- Valid table creation still works.
CREATE TABLE ts_valid ENGINE = TimeSeries;
SELECT 'ok';
DROP TABLE ts_valid;

-- The documented `time_series` type override is honored: the declared tuple element types propagate both to
-- the outer `time_series` column AND inward to the generated samples INNER COLUMNS. Asserting only the outer
-- column is not enough - a regression that kept the outer type but left the inner samples at the defaults
-- (`timestamp DateTime64(3)`, `value Float64`) would still pass. The second SELECT pins the inner schema by
-- extracting the `SAMPLES INNER COLUMNS` block from the stored definition.
CREATE TABLE ts_override (time_series Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_override' AND name = 'time_series';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \(([^)]*)\)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_override';
DROP TABLE ts_override;

-- Extra outer columns other than `time_series` (e.g. `metric_name`, `tags`) are NOT rejected: the outer
-- columns of a TimeSeries table are a regenerated IO interface (they store no data), so their declared types
-- are normalized to the canonical schema rather than kept or rejected. This is intentional - a normalized
-- table's stored definition carries the full canonical outer column list, so re-parsing it on ATTACH,
-- replication, or backup restore must tolerate those column names. The DETACH/ATTACH round-trip below
-- exercises exactly that path.
CREATE TABLE ts_extra_outer (metric_name Int32, tags String) ENGINE = TimeSeries;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_extra_outer' ORDER BY position;
DETACH TABLE ts_extra_outer;
ATTACH TABLE ts_extra_outer;
SELECT 'attach round-trip ok';
DROP TABLE ts_extra_outer;
