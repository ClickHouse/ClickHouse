-- The old analyzer used to install a type-default constant for an always-constant argument hiding
-- behind `__scalarSubqueryResult`, so the declared result type was derived from a zero/empty value
-- while execution used the real one. Pinned explicitly because `compatibility` randomization can
-- flip `enable_analyzer` and silently stop testing the old analyzer.
SET enable_analyzer = 0;
-- Results carrying a timezone-less `DateTime` are rendered in the session timezone, which the test
-- runner randomizes. The timezone-argument rows below name a timezone explicitly and are unaffected.
SET session_timezone = 'UTC';

SELECT '-- scale argument: declared type must match the folded value';
SELECT toTypeName(toDateTime(0, __scalarSubqueryResult(toUInt64(1)))), toDateTime(0, __scalarSubqueryResult(toUInt64(1)))::String;
SELECT toDecimal32(1, __scalarSubqueryResult(toUInt64(1)))::String, toDecimal64(1, __scalarSubqueryResult(toUInt64(3)))::String;
SELECT toDecimal128(1, __scalarSubqueryResult(toUInt64(1)))::String, toDecimal256(1, __scalarSubqueryResult(toUInt64(1)))::String;
SELECT toDateTime64(0, __scalarSubqueryResult(toUInt64(1)))::String;
SELECT toTime64('12:00:00', __scalarSubqueryResult(toUInt64(1)))::String, toTime64('12:00:00', __scalarSubqueryResult(toUInt64(3)))::String;
SELECT divideDecimal(toDecimal64(1, 2), toDecimal64(3, 2), __scalarSubqueryResult(toUInt8(3)))::String;
SELECT length(toFixedString('ab', __scalarSubqueryResult(toUInt8(3))));

SELECT '-- timezone argument';
SELECT toDateTime(1600000000, __scalarSubqueryResult('Asia/Tokyo'))::String;
SELECT toDateTime64(1600000000, 3, __scalarSubqueryResult('Asia/Tokyo'))::String;
SELECT toTypeName(toDateTime('2020-01-01 00:00:00', __scalarSubqueryResult('Asia/Tokyo')));
SELECT toStartOfDay(toDateTime(1600000000), __scalarSubqueryResult('Asia/Tokyo'))::String;

SELECT '-- the analyzer rejects a directly typed wrapper instead of folding it, and still does';
SET enable_analyzer = 1;
SELECT toStartOfDay(toDateTime(1600000000), __scalarSubqueryResult('Asia/Tokyo'))::String; -- { serverError ILLEGAL_COLUMN }
SELECT toDateTime(0, __scalarSubqueryResult(toUInt64(1))); -- { serverError ILLEGAL_COLUMN }
SET enable_analyzer = 0;

SELECT '-- a literal in the same position gives the same answers';
SELECT toTypeName(toDateTime(0, 1)), toDateTime(0, 1)::String;
SELECT toDecimal32(1, 1)::String, toDecimal64(1, 3)::String;
SELECT toTime64('12:00:00', 3)::String, divideDecimal(toDecimal64(1, 2), toDecimal64(3, 2), 3)::String;
SELECT toDateTime(1600000000, 'Asia/Tokyo')::String, length(toFixedString('ab', 3));

SELECT '-- non-foldable children are never executed: still rejected, and no sleep/throwIf runs';
SELECT toDecimal32(1, __scalarSubqueryResult(toUInt64(sleep(0.1)))); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal32(1, __scalarSubqueryResult(sleep(0.1))); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal32(1, __scalarSubqueryResult(toUInt64(rand() % 3))); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal32(1, __scalarSubqueryResult(toUInt64(throwIf(1, 'boom')))); -- { serverError ILLEGAL_COLUMN }

SELECT '-- only __scalarSubqueryResult is looked through, and only genuine constants are accepted';
SELECT toDateTime(0, identity(toUInt64(1))); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal32(1, materialize(toUInt64(1))); -- { serverError ILLEGAL_COLUMN }
SELECT toDateTime(0, __scalarSubqueryResult(toUInt64(0)))::String;

SELECT '-- ordinary path unchanged';
SELECT toDecimal32(1, 2)::String, toDecimal32(1, (SELECT 2))::String;
SELECT toDateTime(1600000000, (SELECT 'Asia/Tokyo'))::String;
SELECT toDecimal32OrZero('1', __scalarSubqueryResult(toUInt64(1)))::String;
SELECT multiplyDecimal(toDecimal64(2, 2), toDecimal64(3, 2), __scalarSubqueryResult(toUInt8(3)))::String;
SELECT round(1.2345, __scalarSubqueryResult(toUInt64(3)))::String;
SELECT toString(toDateTime(1600000000), __scalarSubqueryResult('Asia/Tokyo'));

SELECT '-- an expression above the wrapper keeps the previous behaviour: the wrapper payload is not the argument value';
-- Only the declared type is asserted here: this shape has a pre-existing value divergence of its own
-- (the result depends on whether the expression is JIT-compiled), which is out of scope.
SELECT toTypeName(toDecimal32(1, __scalarSubqueryResult(toUInt64(1)) + 2));

SELECT '-- persisted view/MV column types';
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS tz;
DROP TABLE IF EXISTS sc;
DROP VIEW IF EXISTS v_foldable_scale;
DROP VIEW IF EXISTS v_datadep_scale;
DROP VIEW IF EXISTS v_nested_expr;
DROP TABLE IF EXISTS mv_foldable_tz;
DROP TABLE IF EXISTS mv_datadep_tz;

CREATE TABLE src (x Int64) ENGINE = Log;
CREATE TABLE tz (z String) ENGINE = Log;
CREATE TABLE sc (s UInt8) ENGINE = Log;
INSERT INTO tz VALUES ('Asia/Tokyo');
INSERT INTO sc VALUES (2);

-- (a) foldable timezone: type and stored value now match the analyzer
CREATE MATERIALIZED VIEW mv_foldable_tz ENGINE = Log AS SELECT toDateTime(x, (SELECT 'Asia/Tokyo')) AS d FROM src;
-- (b) data-dependent timezone: the scalar is not known while the view is analyzed, so it stays as it was
CREATE MATERIALIZED VIEW mv_datadep_tz ENGINE = Log AS SELECT toDateTime(x, (SELECT z FROM tz)) AS d FROM src;
-- (c) foldable scale
CREATE VIEW v_foldable_scale AS SELECT toDecimal32(1, (SELECT 2)) AS d FROM src;
-- (d) data-dependent scale
CREATE VIEW v_datadep_scale AS SELECT toDecimal32(1, (SELECT s FROM sc)) AS d FROM src;
-- (e) expression above the wrapper: falls back, so unchanged
CREATE VIEW v_nested_expr AS SELECT toDecimal32(1, (SELECT 2) + 1) AS d FROM src;

SELECT table, type FROM system.columns
WHERE database = currentDatabase() AND name = 'd'
  AND table IN ('mv_foldable_tz', 'mv_datadep_tz', 'v_foldable_scale', 'v_datadep_scale', 'v_nested_expr')
ORDER BY table;

INSERT INTO src VALUES (1600000000);
SELECT 'mv_foldable_tz', d::String FROM mv_foldable_tz;
SELECT 'mv_datadep_tz', d::String FROM mv_datadep_tz;

DROP VIEW v_nested_expr;
DROP VIEW v_datadep_scale;
DROP VIEW v_foldable_scale;
DROP TABLE mv_datadep_tz;
DROP TABLE mv_foldable_tz;
DROP TABLE sc;
DROP TABLE tz;
DROP TABLE src;
