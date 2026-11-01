-- A table TTL is analyzed with the server settings, as at CREATE TABLE: a session setting that changes the
-- types of the TTL expression neither breaks an INSERT nor changes which TTL an ALTER accepts.

SET session_timezone = 'UTC';
SET async_insert = 0;

DROP TABLE IF EXISTS t_cast;
DROP TABLE IF EXISTS t_nullable_ttl;
DROP TABLE IF EXISTS t_dyn;
DROP TABLE IF EXISTS t_json;
DROP TABLE IF EXISTS t_in;
DROP TABLE IF EXISTS t_in_create;
DROP TABLE IF EXISTS t_ext;
DROP TABLE IF EXISTS t_trunc;
DROP TABLE IF EXISTS t_where;
DROP TABLE IF EXISTS t_gb;

-- `cast_keep_nullable` makes `CAST` of a `Nullable` value `Nullable`.
CREATE TABLE t_cast (d DateTime('UTC'), x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(CAST(x, 'UInt8'));
SYSTEM STOP MERGES t_cast;
INSERT INTO t_cast SETTINGS cast_keep_nullable = 1 VALUES ('2100-01-01 00:00:00', 1);
INSERT INTO t_cast SETTINGS cast_keep_nullable = 1 VALUES ('2000-01-01 00:00:00', 1);
INSERT INTO t_cast SETTINGS max_threads = 3, max_block_size = 1000 VALUES ('2100-01-01 00:00:00', 2);
SELECT count() FROM t_cast;
SELECT toString(delete_ttl_info_min, 'UTC'), toString(delete_ttl_info_max, 'UTC')
FROM system.parts WHERE database = currentDatabase() AND table = 't_cast' AND active ORDER BY min_block_number;
SYSTEM START MERGES t_cast;
OPTIMIZE TABLE t_cast FINAL;
SELECT count() FROM t_cast;

INSERT INTO t_cast SETTINGS cast_keep_nullable = 0 VALUES ('2100-01-01 00:00:00', NULL); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
INSERT INTO t_cast SETTINGS cast_keep_nullable = 1 VALUES ('2100-01-01 00:00:00', NULL); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SET cast_keep_nullable = 1, materialize_ttl_after_modify = 0;
ALTER TABLE t_cast MODIFY TTL d + toIntervalDay(CAST(x, 'UInt8') + 1);
ALTER TABLE t_cast ADD COLUMN z UInt8;
SET cast_keep_nullable = 0, materialize_ttl_after_modify = 1;

CREATE TABLE t_nullable_ttl (d DateTime('UTC'), x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(x); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_cast MODIFY TTL d + toIntervalDay(x); -- { serverError BAD_TTL_EXPRESSION }

-- `cast_keep_nullable` also makes a conversion of a `Dynamic` value `Nullable`.
CREATE TABLE t_dyn (d DateTime('UTC'), arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(toUInt8(arrayMap(x -> CAST(1, 'Dynamic'), arr)[1]));
INSERT INTO t_dyn SETTINGS cast_keep_nullable = 1 VALUES ('2100-01-01 00:00:00', [1]);
SELECT count() FROM t_dyn;

-- `function_json_value_return_type_allow_nullable` makes `JSON_VALUE` `Nullable`.
CREATE TABLE t_json (s String) ENGINE = MergeTree ORDER BY tuple()
TTL parseDateTimeBestEffort(JSON_VALUE(s, '$.t')) + INTERVAL 1 DAY;
INSERT INTO t_json SETTINGS function_json_value_return_type_allow_nullable = 1 VALUES ('{"t":"2100-01-01 00:00:00"}');
SELECT count() FROM t_json;

-- `transform_null_in` makes `IN` over a `Nullable` value not `Nullable`.
CREATE TABLE t_in (d DateTime('UTC'), x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 DAY;
SET transform_null_in = 1;
CREATE TABLE t_in_create (d DateTime('UTC'), x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(x IN (1, 2)); -- { serverError BAD_TTL_EXPRESSION }
ALTER TABLE t_in MODIFY TTL d + toIntervalDay(x IN (1, 2)); -- { serverError BAD_TTL_EXPRESSION }
SET transform_null_in = 0;
INSERT INTO t_in VALUES ('2100-01-01 00:00:00', 1);
SELECT count() FROM t_in;

-- `enable_extended_results_for_datetime_functions` makes `toStartOfHour` of a `DateTime64` a `DateTime64`,
-- which `tumbleStart` does not accept.
CREATE TABLE t_ext (ts DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple()
TTL tumbleStart(toStartOfHour(ts), toIntervalHour(1)) + INTERVAL 1 DAY;
INSERT INTO t_ext SETTINGS enable_extended_results_for_datetime_functions = 1 VALUES ('2100-01-01 00:00:00');
SELECT count() FROM t_ext;

-- The `DELETE WHERE` condition and a `GROUP BY ... SET` aggregation are analyzed the same way.
CREATE TABLE t_where (ts DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple()
TTL ts + INTERVAL 1 DAY DELETE WHERE tumbleStart(toStartOfHour(ts), toIntervalHour(1)) > toDateTime('2000-01-01 00:00:00', 'UTC');
INSERT INTO t_where SETTINGS enable_extended_results_for_datetime_functions = 1 VALUES ('2100-01-01 00:00:00');
SELECT count() FROM t_where;
CREATE TABLE t_gb (k UInt64, ts DateTime64(0, 'UTC'), v DateTime('UTC')) ENGINE = MergeTree ORDER BY k TTL ts + INTERVAL 1 DAY;
SET enable_extended_results_for_datetime_functions = 1, materialize_ttl_after_modify = 0;
ALTER TABLE t_where MODIFY TTL ts + INTERVAL 2 DAY DELETE WHERE tumbleStart(toStartOfHour(ts), toIntervalHour(1)) > toDateTime('2000-01-01 00:00:00', 'UTC');
ALTER TABLE t_gb MODIFY TTL ts + INTERVAL 1 DAY GROUP BY k SET v = max(tumbleStart(toStartOfHour(ts), toIntervalHour(1)));
SET enable_extended_results_for_datetime_functions = 0, materialize_ttl_after_modify = 1;

-- `function_date_trunc_return_type_behavior = 1` makes `dateTrunc` of a `DateTime64` a `DateTime`.
CREATE TABLE t_trunc (ts DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY tuple() TTL ts + INTERVAL 1 DAY;
SET function_date_trunc_return_type_behavior = 1;
ALTER TABLE t_trunc MODIFY TTL tumbleStart(dateTrunc('hour', ts), toIntervalHour(1)) + INTERVAL 1 DAY; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET function_date_trunc_return_type_behavior = 0;

DROP TABLE t_cast;
DROP TABLE t_dyn;
DROP TABLE t_json;
DROP TABLE t_in;
DROP TABLE t_ext;
DROP TABLE t_trunc;
DROP TABLE t_where;
DROP TABLE t_gb;
