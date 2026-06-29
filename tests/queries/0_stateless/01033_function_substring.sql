SELECT '-- argument validation';

SELECT substring('hello', []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT substring('hello', 1, []); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT substring(materialize('hello'), -1, -1);
SELECT substring(materialize('hello'), 0); -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }

SELECT '-- FixedString arguments';

SELECT substring(toFixedString('hello', 16), 1, 8);
SELECT substring(toFixedString(materialize('hello'), 16), 1, 8);
SELECT substring(toFixedString(toString(number), 16), 1, 8) FROM system.numbers LIMIT 10;
SELECT substring(toFixedString(toString(number), 4), 1, 3) FROM system.numbers LIMIT 995, 10;
SELECT substring(toFixedString(toString(number), 4), 1, number % 5) FROM system.numbers LIMIT 995, 10;
SELECT substring(toFixedString(toString(number), 4), 1 + number % 5) FROM system.numbers LIMIT 995, 10;
SELECT substring(toFixedString(toString(number), 4), 1 + number % 5, 1 + number % 3) FROM system.numbers LIMIT 995, 10;

SELECT '-- Enum arguments';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab(e8 Enum8('hello' = -5, 'world' = 15), e16 Enum16('shark' = -999, 'eagle' = 9999)) ENGINE MergeTree ORDER BY tuple();
INSERT INTO TABLE tab VALUES ('hello', 'shark'), ('world', 'eagle');

-- positive offsets (slice from left)
SELECT substring(e8, 1), substring (e16, 1) FROM tab;
SELECT substring(e8, 2, 10), substring (e16, 2, 10) FROM tab;
-- negative offsets (slice from right)
SELECT substring(e8, -1), substring (e16, -1) FROM tab;
SELECT substring(e8, -2, 10), substring (e16, -2, 10) FROM tab;
-- zero offset/length
SELECT substring(e8, 1, 0), substring (e16, 1, 0) FROM tab;

SELECT '-- Constant enums';
SELECT substring(CAST('foo', 'Enum8(\'foo\' = 1)'), 1, 1), substring(CAST('foo', 'Enum16(\'foo\' = 1111)'), 1, 2);

DROP TABLE tab;

SELECT '-- negative offset argument';

SELECT substring('abc', number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize('abc'), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(toFixedString('abc', 3), number - 5) FROM system.numbers LIMIT 10;
SELECT substring(materialize(toFixedString('abc', 3)), number - 5) FROM system.numbers LIMIT 10;

SELECT substring('clickhouse', 2, -2);
SELECT substring(materialize('clickhouse'), 2, -2);
SELECT substring('clickhouse', materialize(2), -2);
SELECT substring(materialize('clickhouse'), materialize(2), -2);
SELECT substring('clickhouse', 2, materialize(-2));
SELECT substring(materialize('clickhouse'), 2, materialize(-2));
SELECT substring('clickhouse', materialize(2), materialize(-2));
SELECT substring(materialize('clickhouse'), materialize(2), materialize(-2));

SELECT '-- negative length argument';

SELECT substring('abcdefgh', 2, -2);
SELECT substring('abcdefgh', materialize(2), -2);
SELECT substring('abcdefgh', 2, materialize(-2));
SELECT substring('abcdefgh', materialize(2), materialize(-2));

SELECT substring(cast('abcdefgh' AS FixedString(8)), 2, -2);
SELECT substring(cast('abcdefgh' AS FixedString(8)), materialize(2), -2);
SELECT substring(cast('abcdefgh' AS FixedString(8)), 2, materialize(-2));
SELECT substring(cast('abcdefgh' AS FixedString(8)), materialize(2), materialize(-2));

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, l Int8, r Int8) ENGINE = Memory;
INSERT INTO tab VALUES ('abcdefgh', 2, -2), ('12345678', 3, -3);

SELECT substring(s, 2, -2) FROM tab;
SELECT substring(s, l, -2) FROM tab;
SELECT substring(s, 2, r) FROM tab;
SELECT substring(s, l, r) FROM tab;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s FixedString(8), l Int8, r Int8) ENGINE = Memory;
INSERT INTO tab VALUES ('abcdefgh', 2, -2), ('12345678', 3, -3);

SELECT substring(s, 2, -2) FROM tab;
SELECT substring(s, l, -2) FROM tab;
SELECT substring(s, 2, r) FROM tab;
SELECT substring(s, l, r) FROM tab;

DROP TABLE IF EXISTS tab;

SELECT '-- negative offset and size';

SELECT substring('abcdefgh', -2, -2);
SELECT substring(materialize('abcdefgh'), -2, -2);
SELECT substring(materialize('abcdefgh'), materialize(-2), materialize(-2));

SELECT substring('abcdefgh', -2, -1);
SELECT substring(materialize('abcdefgh'), -2, -1);
SELECT substring(materialize('abcdefgh'), materialize(-2), materialize(-1));

SELECT substring(cast('abcdefgh' AS FixedString(8)), -2, -2);
SELECT substring(materialize(cast('abcdefgh' AS FixedString(8))), -2, -2);
SELECT substring(materialize(cast('abcdefgh' AS FixedString(8))), materialize(-2), materialize(-2));

SELECT substring(cast('abcdefgh' AS FixedString(8)), -2, -1);
SELECT substring(materialize(cast('abcdefgh' AS FixedString(8))), -2, -1);
SELECT substring(materialize(cast('abcdefgh' AS FixedString(8))), materialize(-2), materialize(-1));

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    s String,
    l Int8,
    r Int8
) ENGINE = Memory;

INSERT INTO t VALUES ('abcdefgh', -2, -2),('12345678', -3, -3);

SELECT substring(s, -2, -2) FROM t;
SELECT substring(s, l, -2) FROM t;
SELECT substring(s, -2, r) FROM t;
SELECT substring(s, l, r) FROM t;

SELECT '-';
DROP TABLE IF EXISTS t;
CREATE TABLE t(
                  s FixedString(8),
                  l Int8,
                  r Int8
) engine = Memory;
INSERT INTO t VALUES ('abcdefgh', -2, -2),('12345678', -3, -3);

SELECT substring(s, -2, -2) FROM t;
SELECT substring(s, l, -2) FROM t;
SELECT substring(s, -2, r) FROM t;
SELECT substring(s, l, r) FROM t;

DROP table if exists t;

SELECT '-- UBSAN bug';

/** NOTE: The behaviour of substring and substringUTF8 is inconsistent when negative offset is greater than string size:
  * substring:
  *      hello
  * ^-----^ - offset -10, length 7, result: "he"
  * substringUTF8:
  *      hello
  *      ^-----^ - offset -10, length 7, result: "hello"
  * This may be subject for change.
  */
SELECT substringUTF8('hello, Ð¿Ñ�Ð¸Ð²ÐµÑ�', -9223372036854775808, number) FROM numbers(16) FORMAT Null;

SELECT '-- Alias';
SELECT byteSlice('hello', 2, 2);
