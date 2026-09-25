-- A: unparsable text returns NULL for the targets below, as it already did for a String source.
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Int32');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'UInt16');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Float64');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Decimal64(2)');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Date');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Date32');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'DateTime');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'DateTime64(3)');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Time');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Time64(3)');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'UUID');

-- B: a value the target cannot represent returns NULL instead of a wrapped one.
SELECT accurateCastOrNull(materialize(toFixedString('300', 12)), 'Int8');
SELECT accurateCastOrNull(materialize(toFixedString('65536', 12)), 'UInt16');
SELECT accurateCastOrNull(materialize(toFixedString('-1', 12)), 'UInt8');
SELECT accurateCastOrNull(materialize(toFixedString('99999999999999999999', 24)), 'Int32');
SELECT accurateCastOrNull(materialize(toFixedString('128', 12)), 'Int8');
SELECT accurateCastOrNull(materialize(toFixedString('-129', 12)), 'Int8');

-- D: parseable values are unchanged, and the zero padding is still tolerated.
SELECT accurateCastOrNull(materialize(toFixedString('7', 12)), 'Int32');
SELECT accurateCastOrNull(materialize(toFixedString('+7', 12)), 'Int32');
SELECT accurateCastOrNull(materialize(toFixedString('3.14', 12)), 'Float64');
SELECT accurateCastOrNull(materialize(toFixedString('2020-01-01', 12)), 'Date');
SELECT accurateCastOrNull(materialize(toFixedString('65535', 12)), 'UInt16');
SELECT accurateCastOrNull(materialize(toFixedString('127', 12)), 'Int8');
SELECT accurateCastOrNull(materialize(toFixedString('-128', 12)), 'Int8');
SELECT accurateCastOrNull(materialize(toFixedString('00000000-0000-0000-0000-000000000000', 36)), 'UUID');

-- E: plain CAST to a Nullable target behaves the same way.
SELECT CAST(materialize(toFixedString('abc', 12)) AS Nullable(Int32));
SELECT CAST(materialize(toFixedString('300', 12)) AS Nullable(Int8));

-- F: accurateCastOrDefault returns the default instead of throwing.
SELECT accurateCastOrDefault(materialize(toFixedString('abc', 12)), 'Int32');
SELECT accurateCastOrDefault(materialize(toFixedString('abc', 12)), 'Int32', CAST(42, 'Int32'));

-- G: accurateCast still rejects, with or without a Nullable target.
SELECT accurateCast(materialize(toFixedString('abc', 12)), 'Int32'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize(toFixedString('abc', 12)), 'Nullable(Int32)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize(toFixedString('300', 12)), 'Int8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize(toFixedString('abc', 12)), 'Decimal64(2)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize(toFixedString('abc', 12)), 'Nullable(Decimal64(2))'); -- { serverError CANNOT_PARSE_TEXT }

-- H: a FixedString of the exact binary width is still read as bytes, padding included.
SELECT accurateCastOrNull(materialize(toFixedString('abc', 16)), 'UUID');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 16)), 'IPv6');

-- I: wrapped sources reach the same parser.
SELECT accurateCastOrNull(toLowCardinality(materialize(toFixedString('abc', 12))), 'Int32');
SELECT accurateCastOrNull(CAST(materialize(toFixedString('abc', 12)) AS Nullable(FixedString(12))), 'Int32');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Int32') SETTINGS cast_keep_nullable = 1;

-- J: independent oracle. accurateCastOrNull must agree with to<T>OrNull, which is already correct
-- for a FixedString source and is not changed by this fix for these targets. Only 1s.
SELECT
    isNotDistinctFrom(accurateCastOrNull(x, 'Int8'), toInt8OrNull(x)),
    isNotDistinctFrom(accurateCastOrNull(x, 'UInt16'), toUInt16OrNull(x)),
    isNotDistinctFrom(accurateCastOrNull(x, 'Int32'), toInt32OrNull(x)),
    isNotDistinctFrom(accurateCastOrNull(x, 'Float64'), toFloat64OrNull(x))
FROM (SELECT toFixedString(arrayJoin(['abc', '300', '-1', '7', '+7', '65535']), 24) AS x)
ORDER BY x;

-- K: the Decimal* family. The trailing zero padding of a FixedString is not part of the value,
-- so the non-throwing decimal reader must not reject it.
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Decimal32(2)');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Decimal128(4)');
SELECT accurateCastOrNull(materialize(toFixedString('abc', 12)), 'Decimal256(0)');
SELECT accurateCastOrNull(materialize(toFixedString('99999999999999999999', 24)), 'Decimal32(2)');
SELECT CAST(materialize(toFixedString('abc', 12)) AS Nullable(Decimal64(2)));
SELECT accurateCastOrDefault(materialize(toFixedString('abc', 12)), 'Decimal64(2)');
SELECT toDecimal64OrNull(materialize(toFixedString('3.14', 12)), 2);
SELECT toDecimal64OrZero(materialize(toFixedString('3.14', 12)), 2);
SELECT toDecimal256OrNull(materialize(toFixedString('3', 12)), 0);
SELECT toDecimal32OrNull(materialize(toFixedString('1.5', 20)), 2);
SELECT toDecimal128OrNull(materialize(toFixedString('-2.25', 20)), 4);
SELECT accurateCastOrNull(materialize(toFixedString('3.14', 12)), 'Decimal64(2)');
SELECT accurateCastOrNull(materialize(toFixedString('3.14', 4)), 'Decimal64(2)');
SELECT accurateCastOrNull(materialize(toFixedString('3', 12)), 'Decimal256(0)');
SELECT CAST(materialize(toFixedString('3.14', 12)) AS Nullable(Decimal64(2)));
SELECT accurateCastOrDefault(materialize(toFixedString('3.14', 12)), 'Decimal64(2)');
SELECT accurateCastOrNull(materialize(toFixedString('2020-01-01 00:00:00', 30)), 'DateTime64(3)');
SELECT accurateCastOrNull(materialize(toFixedString('12:00:00', 20)), 'Time64(3)');
SELECT toDecimal64OrNull(materialize(toFixedString('3.14', 4)), 2);
SELECT toDecimal64OrNull(materialize(toFixedString('3.14abc', 12)), 2);
SELECT toDecimal64OrNull(materialize(toFixedString('', 12)), 2);
SELECT toDecimal64OrNull(CAST(concat(toFixedString('3.14', 6), 'x') AS FixedString(12)), 2);
SELECT toDecimal64OrNull(materialize('3.14abc'), 2);
SELECT toDecimal64OrNull(materialize('3.14'), 2);

-- L: KeyCondition prepares an IN set with accurateCastOrNull, so an element the key type cannot
-- represent is filtered out of the set instead of failing the query, as it already was for a String.
DROP TABLE IF EXISTS t_05230_key;
-- add_minmax_index_for_numeric_columns=0: an implicit index repeats the condition in the plan below.
CREATE TABLE t_05230_key (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_05230_key SELECT number FROM numbers(10);
SELECT count() FROM t_05230_key WHERE k IN (SELECT toFixedString('not a number', 12));
SELECT count() FROM t_05230_key WHERE k IN (SELECT materialize('not a number'));
-- The set is built from one row, so a primary-key set of zero elements is that element being dropped.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_05230_key WHERE k IN (SELECT toFixedString('not a number', 12))) WHERE explain ILIKE '%k in 0-element set%';
DROP TABLE t_05230_key;
