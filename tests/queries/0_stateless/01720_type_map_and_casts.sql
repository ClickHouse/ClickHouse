DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (d DATE, m Map(Int8, Int8))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map(127, 1, 0, 1, -1, 1)) ('2020-01-01', map());

SELECT 'Map(Int8, Int8)';

SELECT m FROM table_map_with_key_integer;
SELECT m[127], m[1], m[0], m[-1] FROM table_map_with_key_integer;
SELECT m[toInt8(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3, 4] AS number;

SELECT count() FROM table_map_with_key_integer WHERE m = map();

DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (d DATE, m Map(Int32, UInt16))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map(-1, 1, 2147483647, 2, -2147483648, 3));

SELECT 'Map(Int32, UInt16)';

SELECT m FROM table_map_with_key_integer;
SELECT m[-1], m[2147483647], m[-2147483648] FROM table_map_with_key_integer;
SELECT m[toInt32(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3, 4] AS number;

DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (d DATE, m Map(Date, Int32))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map('2020-01-01', 1, '2020-01-02', 2, '1970-01-02', 3));

SELECT 'Map(Date, Int32)';

SELECT m FROM table_map_with_key_integer;
SELECT m[toDate('2020-01-01')], m[toDate('2020-01-02')], m[toDate('2020-01-03')] FROM table_map_with_key_integer;
SELECT m[toDate(number)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2] AS number;

DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (d DATE, m Map(UUID, UInt16))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map('00001192-0000-4000-8000-000000000001', 1, '00001192-0000-4000-7000-000000000001', 2));

SELECT 'Map(UUID, UInt16)';

SELECT m FROM table_map_with_key_integer;
SELECT
    m[toUUID('00001192-0000-4000-6000-000000000001')],
    m[toUUID('00001192-0000-4000-7000-000000000001')],
    m[toUUID('00001192-0000-4000-8000-000000000001')]
FROM table_map_with_key_integer;

SELECT m[257], m[1] FROM table_map_with_key_integer; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (d DATE, m Map(Int128, String))
ENGINE = MergeTree() ORDER BY d;


INSERT INTO table_map_with_key_integer SELECT '2020-01-01', map(-1, 'a', 0, 'b', toInt128('1234567898765432123456789'), 'c', toInt128('-1234567898765432123456789'), 'd');

SELECT 'Map(Int128, String)';

SELECT m FROM table_map_with_key_integer;
SELECT m[toInt128(-1)], m[toInt128(0)], m[toInt128('1234567898765432123456789')], m[toInt128('-1234567898765432123456789')] FROM table_map_with_key_integer;
SELECT m[toInt128(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3] AS number;

SELECT m[-1], m[0], m[toInt128('1234567898765432123456789')], m[toInt128('-1234567898765432123456789')] FROM table_map_with_key_integer;
SELECT m[toUInt64(0)], m[toInt64(0)], m[toUInt8(0)], m[toUInt16(0)] FROM table_map_with_key_integer;

DROP TABLE IF EXISTS table_map_with_key_integer;


CREATE TABLE table_map_with_key_integer (m Map(Float32, String)) ENGINE = MergeTree() ORDER BY tuple();
DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (m Map(Array(UInt32), String)) ENGINE = MergeTree() ORDER BY tuple();
DROP TABLE IF EXISTS table_map_with_key_integer;

CREATE TABLE table_map_with_key_integer (m Map(Nullable(String), String)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS}
