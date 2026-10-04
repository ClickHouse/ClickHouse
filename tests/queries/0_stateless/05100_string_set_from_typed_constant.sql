-- A constant of another type converted to `String` - which is what building the set of `IN` against a
-- `String` column does - was rendered as a query literal rather than with the type's own text
-- serialization: a `Date` came out as its day number, a `Float64` as `1.`, and a `UUID` or an `IPv4`
-- carried the quote characters of the literal inside the string. `IN` then disagreed with
-- `CAST(x AS String)` and filtered on garbage.

SELECT 'the text of the value is what the set holds';
SELECT toString(toDate('2020-01-01')) IN (toDate('2020-01-01')), '18262' IN (toDate('2020-01-01'));
SELECT toString(toDateTime('2020-01-01 00:00:00', 'UTC')) IN (toDateTime('2020-01-01 00:00:00', 'UTC'));
SELECT '1.2.3.4' IN (toIPv4('1.2.3.4')), '::1' IN (toIPv6('::1'));
SELECT '00000000-0000-0000-0000-000000000001' IN (toUUID('00000000-0000-0000-0000-000000000001'));
SELECT '1.5' IN (toDecimal32(1.5, 1)), '1' IN (toFloat64(1));
SELECT 'true' IN (true), '1' IN (toUInt64(1));

SELECT 'and the values table function renders the same text as CAST';
SELECT x FROM values('x String', toDate('2020-01-01'));
SELECT CAST(toDate('2020-01-01'), 'String');
SELECT x FROM values('x String', toUUID('00000000-0000-0000-0000-000000000001'));
SELECT x FROM values('x String', toIPv4('1.2.3.4'));
SELECT x FROM values('x String', toFloat64(1));

SELECT 'over a table, where the set filters rows';
DROP TABLE IF EXISTS t_string_set;
CREATE TABLE t_string_set (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_string_set VALUES ('2020-01-01'), ('18262'), ('1.2.3.4'), ('zz');

SELECT v FROM t_string_set WHERE v IN (toDate('2020-01-01')) ORDER BY v;
SELECT v FROM t_string_set WHERE v IN (toIPv4('1.2.3.4')) ORDER BY v;
SELECT v FROM t_string_set WHERE v NOT IN (toDate('2020-01-01')) ORDER BY v;

SELECT 'a String constant is unchanged';
SELECT v FROM t_string_set WHERE v IN ('18262') ORDER BY v;
SELECT 'zz' IN ('zz'), 'zz' IN ('aa');

SELECT 'an Enum constant keeps the name, as it already did';
SELECT 'a' IN (CAST('a', 'Enum8(''a'' = 1)')), '1' IN (CAST('a', 'Enum8(''a'' = 1)'));

-- The set is rendered as `CAST(x AS String)` renders the value under the same session settings: `CAST`
-- writes dates and times with fixed text, so `date_time_output_format` changes neither, while it
-- serializes a `Bool` with the query's settings, so `bool_true_representation` changes both together.
SELECT 'the session output format settings apply exactly where CAST applies them';
SET date_time_output_format = 'unix_timestamp';
SELECT CAST(toDateTime('2020-01-01 00:00:00', 'UTC'), 'String');
SELECT '2020-01-01 00:00:00' IN (toDateTime('2020-01-01 00:00:00', 'UTC')), '1577836800' IN (toDateTime('2020-01-01 00:00:00', 'UTC'));
SELECT ('2020-01-01 00:00:00', 1) IN ((toDateTime('2020-01-01 00:00:00', 'UTC'), 1)), ('1577836800', 1) IN ((toDateTime('2020-01-01 00:00:00', 'UTC'), 1));
SELECT x FROM values('x String', toDateTime('2020-01-01 00:00:00', 'UTC'));
INSERT INTO t_string_set VALUES ('2020-01-01 00:00:00'), ('1577836800');
SELECT v FROM t_string_set WHERE v IN (toDateTime('2020-01-01 00:00:00', 'UTC')) ORDER BY v;
SET date_time_output_format = 'simple';

SET bool_true_representation = 'yes';
SELECT CAST(true, 'String');
SELECT 'yes' IN (true), 'true' IN (true);
SELECT ('yes', 1) IN ((true, 1)), ('true', 1) IN ((true, 1));
SELECT x FROM values('x String', true);
INSERT INTO t_string_set VALUES ('yes'), ('true');
SELECT v FROM t_string_set WHERE v IN (true) ORDER BY v;
SET bool_true_representation = 'true';

-- A composite constant renders each nested element as `CAST` does, so the source element, key and value
-- types are carried into the recursion rather than lost at the first level.
SELECT 'nested elements of arrays, tuples and maps are rendered as CAST renders them';
SELECT CAST([toDate('2020-01-01')], 'Array(String)');
SELECT ['2020-01-01'] IN ([toDate('2020-01-01')]), ['18262'] IN ([toDate('2020-01-01')]);
SELECT x FROM values('x Array(String)', [toDate('2020-01-01')]);
SELECT x FROM values('x Array(Array(String))', [[toIPv4('1.2.3.4')]]);
SELECT x FROM values('x Tuple(String, String)', (toDate('2020-01-01'), toUUID('00000000-0000-0000-0000-000000000001')));
SELECT CAST(map(toDate('2020-01-01'), true), 'Map(String, String)');
SELECT x FROM values('x Map(String, String)', map(toDate('2020-01-01'), true));
SET bool_true_representation = 'yes';
SELECT CAST([true], 'Array(String)'), CAST(map('k', true), 'Map(String, String)');
SELECT ['yes'] IN ([true]), ['true'] IN ([true]);
SELECT x FROM values('x Map(String, String)', map('k', true));
SET bool_true_representation = 'true';

-- `CAST` writes a `Decimal` with fixed text as well, so the decimal output settings do not reach the set.
SELECT 'decimal output settings do not apply, as in CAST';
SET output_format_decimal_trailing_zeros = 1;
SELECT CAST(toDecimal32(1.5, 2), 'String');
SELECT '1.5' IN (toDecimal32(1.5, 2)), '1.50' IN (toDecimal32(1.5, 2));
SELECT x FROM values('x String', toDecimal32(1.5, 2));
SET output_format_decimal_trailing_zeros = 0;

DROP TABLE t_string_set;
