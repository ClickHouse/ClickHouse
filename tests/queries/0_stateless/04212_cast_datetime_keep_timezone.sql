-- https://github.com/ClickHouse/ClickHouse/issues/55072
-- CAST to DateTime/DateTime64 without an explicit time zone should preserve
-- the time zone of the source DateTime/DateTime64 argument, matching the
-- behavior of toDateTime / toDateTime64.

SELECT '-- DateTime source, DateTime target without time zone';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(x), toTypeName(CAST(x, 'DateTime')), toTypeName(x::DateTime), toTypeName(CAST(x AS DateTime));

SELECT '-- DateTime source with explicit time zone, DateTime target with explicit time zone (target wins)';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(CAST(x, 'DateTime(\'UTC\')'));

SELECT '-- DateTime source without explicit time zone, DateTime target without time zone';
WITH toDateTime('2023-01-02 03:04:05') AS x
SELECT toTypeName(CAST(x, 'DateTime')) = toTypeName(x);

SELECT '-- DateTime64 source, DateTime64 target without time zone';
WITH toDateTime64('2023-01-02 03:04:05', 3, 'America/Los_Angeles') AS x
SELECT toTypeName(x), toTypeName(CAST(x, 'DateTime64(3)')), toTypeName(x::DateTime64(6));

SELECT '-- DateTime64 source, DateTime target without time zone (cross-type)';
WITH toDateTime64('2023-01-02 03:04:05', 3, 'America/Los_Angeles') AS x
SELECT toTypeName(CAST(x, 'DateTime'));

SELECT '-- DateTime source, DateTime64 target without time zone (cross-type)';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(CAST(x, 'DateTime64(3)'));

SELECT '-- Nullable wrappers';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(CAST(x, 'Nullable(DateTime)'));
SELECT toTypeName(CAST(toNullable(toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles')), 'DateTime'));

SELECT '-- LowCardinality wrappers';
SELECT toTypeName(CAST(CAST(toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles'), 'LowCardinality(DateTime(\'America/Los_Angeles\'))'), 'DateTime')) SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT '-- Non-DateTime source: target stays unchanged';
SELECT toTypeName(CAST(42, 'DateTime'));
SELECT toTypeName(CAST('2023-01-02 03:04:05', 'DateTime'));

SELECT '-- Values are unchanged: only the type carries the time zone';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT x = CAST(x, 'DateTime'), CAST(x, 'DateTime') = toDateTime(x);

SELECT '-- toDateTime function behavior matches CAST behavior';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(toDateTime(x)) = toTypeName(CAST(x, 'DateTime'));

SELECT '-- accurateCast preserves the source time zone';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCast(x, 'DateTime'));
WITH toDateTime64('2023-01-02 03:04:05', 3, 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCast(x, 'DateTime64(6)'));

SELECT '-- accurateCast: target with explicit time zone wins over source';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCast(x, 'DateTime(\'UTC\')'));

SELECT '-- accurateCastOrNull preserves the source time zone (result is Nullable)';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCastOrNull(x, 'DateTime'));
WITH toDateTime64('2023-01-02 03:04:05', 3, 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCastOrNull(x, 'DateTime64(6)'));

SELECT '-- accurateCastOrNull: target with explicit time zone wins over source';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCastOrNull(x, 'DateTime(\'UTC\')'));

SELECT '-- accurateCast / accurateCastOrNull look through Nullable and LowCardinality wrappers';
WITH toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles') AS x
SELECT toTypeName(accurateCast(toNullable(x), 'DateTime'));
SELECT toTypeName(accurateCastOrNull(toNullable(toDateTime('2023-01-02 03:04:05', 'America/Los_Angeles')), 'DateTime'));
