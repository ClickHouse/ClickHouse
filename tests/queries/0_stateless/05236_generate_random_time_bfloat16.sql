-- Types `Time`, `Time64` and `BFloat16` in `generateRandom`.

SELECT toTypeName(t), toTypeName(t64), toTypeName(b)
FROM generateRandom('t Time, t64 Time64(3), b BFloat16', 1)
LIMIT 1;

-- `Time` and `Time64` values are generated inside the representable range.
-- Never compare the printed text: it saturates at the boundary instead of wrapping.
SELECT min(toInt32(t)) >= -3599999, max(toInt32(t)) <= 3599999
FROM (SELECT * FROM generateRandom('t Time', 1) LIMIT 10000);

SELECT min(t64) >= '-999:59:59.999'::Time64(3), max(t64) <= '999:59:59.999'::Time64(3)
FROM (SELECT * FROM generateRandom('t64 Time64(3)', 1) LIMIT 10000);

SELECT count(), min(t64) >= '-999:59:59.999999999'::Time64(9), max(t64) <= '999:59:59.999999999'::Time64(9)
FROM (SELECT * FROM generateRandom('t64 Time64(9)', 1) LIMIT 10000);

-- A `BFloat16` is filled with 16 random bits, so about one value in 256 has an all-ones exponent
-- and is not finite, and about half of them are negative. One stream and one block pin the exact
-- counts: with several streams each of them draws from its own generator, and a block consumes a
-- whole number of 64-bit draws and discards the tail, so both the number of streams and the block
-- size change which values the rows get.
SELECT count(), countIf(isFinite(b)), countIf(b < 0)
FROM (SELECT * FROM generateRandom('b BFloat16', 1) LIMIT 10000 SETTINGS max_threads = 1, max_block_size = 10000);

-- The same seed always produces the same values.
SELECT * FROM generateRandom('t Time, t64 Time64(3), b BFloat16', 1) LIMIT 3 SETTINGS max_threads = 1;

-- Composite types.
SELECT count(), countIf(t IS NULL) > 0
FROM (SELECT * FROM generateRandom('t Nullable(Time)', 1) LIMIT 10000);

SELECT count() > 0, min(toInt64(x)) >= -3599999, max(toInt64(x)) <= 3599999
FROM (SELECT arrayJoin(a) AS x FROM (SELECT * FROM generateRandom('a Array(Time64(6))', 1) LIMIT 10000));

DROP TABLE IF EXISTS t_05236;
CREATE TABLE t_05236 (t Time, t64 Time64(3), b BFloat16) ENGINE = GenerateRandom(1);
SELECT count(), min(toInt32(t)) >= -3599999, max(toInt32(t)) <= 3599999 FROM (SELECT * FROM t_05236 LIMIT 10000);
DROP TABLE t_05236;
