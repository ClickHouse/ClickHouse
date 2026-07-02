-- Test: exercises `TimeSeries` column-type validation error paths during table creation.
-- Covers `normalizeTimeSeriesDefinition`:
--   - the prealpha columns `id`, `timestamp`, `value` are rejected from the outer column list (INCORRECT_QUERY);
--   - the `time_series` column must have type `Array(Tuple(timestamp, value))` (BAD_TYPE_OF_FIELD);
--   - the timestamp and value element types are validated (BAD_TYPE_OF_FIELD).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_reserved_id;
DROP TABLE IF EXISTS ts_reserved_timestamp;
DROP TABLE IF EXISTS ts_reserved_value;
DROP TABLE IF EXISTS ts_bad_shape;
DROP TABLE IF EXISTS ts_bad_timestamp_type;
DROP TABLE IF EXISTS ts_bad_value_type;
DROP TABLE IF EXISTS ts_valid;

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
