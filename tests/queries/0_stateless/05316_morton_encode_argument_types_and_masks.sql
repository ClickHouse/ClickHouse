-- mortonEncode with arguments of every unsigned width, constant and per-row range masks, constant arguments, several blocks

SET max_block_size = 1000;

DROP VIEW IF EXISTS v;
-- h0..h7: full-width values, plus 0 and the maximum
CREATE VIEW v AS
SELECT
    number,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 0)) AS h0,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 1)) AS h1,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 2)) AS h2,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 3)) AS h3,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 4)) AS h4,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 5)) AS h5,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 6)) AS h6,
    multiIf(number % 97 = 0, 0, number % 89 = 0, 18446744073709551615, cityHash64(number, 7)) AS h7
FROM numbers(10000);

SELECT sum(cityHash64(mortonEncode(toUInt8(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt16(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt32(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt64(h0)))) FROM v;

SELECT sum(cityHash64(mortonEncode(toUInt8(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt64(h0), toUInt8(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt16(h0), toUInt32(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt32(h0), toUInt16(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt32(h0), toUInt32(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt64(h0), toUInt64(h1)))) FROM v;

SELECT sum(cityHash64(mortonEncode(toUInt8(h0), toUInt32(h1), toUInt64(h2)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt8(h0), toUInt16(h1), toUInt32(h2), toUInt64(h3), toUInt8(h4), toUInt16(h5), toUInt32(h6), toUInt64(h7)))) FROM v;

SELECT sum(cityHash64(mortonEncode(tuple(1), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(2), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(3), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(4), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(5), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(6), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(7), toUInt64(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode(tuple(8), toUInt64(h0)))) FROM v;

SELECT sum(cityHash64(mortonEncode((1, 8), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((2, 7), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((3, 6), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((4, 5), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((5, 4), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((6, 3), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((7, 2), toUInt32(h0), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((8, 1), toUInt32(h0), toUInt64(h1)))) FROM v;

SELECT sum(cityHash64(mortonEncode((1, 2, 3), toUInt16(h0), toUInt64(h1), toUInt32(h2)))) FROM v;
SELECT sum(cityHash64(mortonEncode((8, 7, 6, 5, 4, 3, 2, 1), toUInt64(h0), toUInt32(h1), toUInt16(h2), toUInt8(h3), toUInt64(h4), toUInt32(h5), toUInt16(h6), toUInt8(h7)))) FROM v;

SELECT sum(cityHash64(mortonEncode(tuple(toUInt8(1 + number % 8)), toUInt32(h0)))) FROM v;
SELECT sum(cityHash64(mortonEncode((1 + number % 8, toUInt16(1 + intDiv(number, 8) % 8)), toUInt64(h0), toUInt16(h1)))) FROM v;

SELECT sum(cityHash64(mortonEncode(5, toUInt32(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((1, 2), 1024, toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode(toUInt64(h0), 255, toUInt16(h2)))) FROM v;

SELECT sum(cityHash64(mortonEncode(if(number % 5 = 0, NULL, toUInt32(h0)), toUInt64(h1)))) FROM v;
SELECT sum(cityHash64(mortonEncode((2, 1), toLowCardinality(toUInt16(h0)), toUInt64(h1)))) FROM v;

DROP VIEW v;
