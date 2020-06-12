SELECT bitCount(number) FROM numbers(10);
SELECT avg(bitCount(number)) FROM numbers(256);

SELECT bitCount(0);
SELECT bitCount(1);
SELECT bitCount(-1);

SELECT bitCount(toInt64(-1));
SELECT bitCount(toInt32(-1));
SELECT bitCount(toInt16(-1));
SELECT bitCount(toInt8(-1));

SELECT x, bitCount(x), hex(reinterpretAsString(x)) FROM VALUES ('x Float64', (1), (-1), (inf));
