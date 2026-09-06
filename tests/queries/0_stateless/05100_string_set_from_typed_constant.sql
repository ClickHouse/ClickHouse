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

DROP TABLE t_string_set;
