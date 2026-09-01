-- https://github.com/ClickHouse/ClickHouse/issues/117215
-- https://github.com/ClickHouse/ClickHouse/issues/117216
-- `useDefaultImplementationForLowCardinalityColumns` unwraps `LowCardinality` arguments for the
-- result type computation, but `buildImpl` used to receive the original wrapped types, so an
-- overload resolver branching on an argument type picked a different implementation than the one its
-- own declared result type describes.

SELECT 'least and greatest';
SELECT greatest(toLowCardinality(materialize(toUInt64(1))), materialize(toInt64(-1)));
SELECT greatest(materialize(toUInt64(1)), materialize(toInt64(-1)));
SELECT least(toLowCardinality(materialize(toUInt64(18446744073709551614))), materialize(toInt64(5)));
SELECT least(materialize(toUInt64(18446744073709551614)), materialize(toInt64(5)));
SELECT toTypeName(greatest(toLowCardinality(materialize(toUInt64(1))), materialize(toInt64(-1))));

SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_lc_resolution;
CREATE TABLE t_lc_resolution (id UInt64, u LowCardinality(UInt64), n LowCardinality(Nullable(UInt64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_resolution VALUES (1, 1, NULL), (2, 7, 3);
SELECT greatest(u, materialize(toInt64(-1))) FROM t_lc_resolution ORDER BY id;

SELECT 'division by a LowCardinality(Nullable) denominator';
SELECT intDiv(10, n) FROM t_lc_resolution ORDER BY id;
SELECT 10 % n FROM t_lc_resolution ORDER BY id;
SELECT positiveModulo(10, n) FROM t_lc_resolution ORDER BY id;
DROP TABLE t_lc_resolution;

SELECT intDiv(10, toLowCardinality(materialize(CAST(NULL, 'Nullable(UInt64)'))));
SELECT 10 % toLowCardinality(materialize(CAST(NULL, 'Nullable(UInt64)')));
SELECT positiveModulo(10, toLowCardinality(materialize(CAST(NULL, 'Nullable(UInt64)'))));
SELECT intDivOrZero(10, toLowCardinality(materialize(CAST(NULL, 'Nullable(UInt64)'))));
SELECT intDiv(10, toLowCardinality(materialize(toUInt64(0)))); -- { serverError ILLEGAL_DIVISION }

SELECT 'other resolvers that branch on the argument type';
SELECT fromModifiedJulianDay(toLowCardinality(materialize(toInt32(58849))));
SELECT fromModifiedJulianDayOrNull(toLowCardinality(materialize(toInt32(58849))));
SELECT runningConcurrency(toLowCardinality(materialize(toDateTime('2020-01-01 00:00:00'))), toLowCardinality(materialize(toDateTime('2020-01-01 00:00:01'))));

SELECT 'constant arguments';
SELECT hasPhrase(materialize('a b c'), toLowCardinality('b c'));
SELECT hasPhrase(materialize('a b c'), toLowCardinality('c b'));
SELECT now(toLowCardinality('UTC')) > toDateTime('2020-01-01', 'UTC');
SELECT CAST(1, toLowCardinality('String'));
SELECT toTypeName(CAST(1, toLowCardinality('String')));
SELECT formatRow(toLowCardinality('CSV'), materialize(1), materialize('a'));
SELECT accurateCastOrNull(1, toLowCardinality('String'));

SELECT 'the plain and LowCardinality forms agree';
SELECT fromModifiedJulianDay(toLowCardinality(materialize(toInt32(58849)))) = fromModifiedJulianDay(materialize(toInt32(58849)));
SELECT hasPhrase(materialize('a b c'), toLowCardinality('b c')) = hasPhrase(materialize('a b c'), 'b c');
