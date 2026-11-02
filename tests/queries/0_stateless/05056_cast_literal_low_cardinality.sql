-- `LowCardinality` hands the text over to the type it wraps, the same as `Nullable` and `Array`:
-- the conversion of a field strips `LowCardinality` before looking at the value, so a numeral would
-- otherwise take the same lossy `Float64` path as with the wrapped type alone.
-- https://github.com/ClickHouse/ClickHouse/issues/116025

SET session_timezone = 'UTC';
SET allow_suspicious_low_cardinality_types = 1;

-- All the ways of writing the cast agree, and keep every digit.
SELECT CAST(607668569663131286404589520 AS LowCardinality(UInt128));
SELECT 607668569663131286404589520::LowCardinality(UInt128);
SELECT CAST('607668569663131286404589520' AS LowCardinality(UInt128));
SELECT CAST(607668569663131286404589520 AS LowCardinality(UInt128)) = CAST('607668569663131286404589520' AS LowCardinality(UInt128));

SELECT CAST(-12345678901234567890123456789 AS LowCardinality(Int256));

-- Composed with the other wrappers.
SELECT CAST(607668569663131286404589520 AS LowCardinality(Nullable(UInt128)));
SELECT CAST([607668569663131286404589520, NULL] AS Array(LowCardinality(Nullable(UInt128))));

-- The types that convert the number keep converting it.
SELECT CAST(1 AS LowCardinality(DateTime));
SELECT CAST(1 AS LowCardinality(UInt8));
