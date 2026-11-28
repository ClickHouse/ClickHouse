-- bitmapContains with a constant bitmap keeps a lookup context across rows.
-- Check that the result does not depend on the order of the lookups or on crossing roaring container boundaries (65536 values).

-- Large bitmap (roaring): every 3rd value in [0, 300000), spanning several containers.
WITH (SELECT groupBitmapState(toUInt32(number * 3)) FROM numbers(100000)) AS bm
SELECT sum(bitmapContains(bm, toUInt32(number))), count() FROM numbers(300000);

-- Same lookups in descending order.
WITH (SELECT groupBitmapState(toUInt32(number * 3)) FROM numbers(100000)) AS bm
SELECT sum(bitmapContains(bm, toUInt32(299999 - number))), count() FROM numbers(300000);

-- Lookups jumping between containers on every row.
WITH (SELECT groupBitmapState(toUInt32(number * 3)) FROM numbers(100000)) AS bm
SELECT sum(bitmapContains(bm, toUInt32((number * 65537) % 300000))) FROM numbers(300000);

-- Sparse bitmap: most containers are absent, both for sorted and for random lookups. Compare with the per-row path.
WITH (SELECT groupBitmapState(toUInt32(cityHash64(number))) FROM numbers(1000)) AS bm
SELECT
    sum(bitmapContains(bm, toUInt32(number * 4096))) = sum(bitmapContains(materialize(bm), toUInt32(number * 4096))),
    sum(bitmapContains(bm, toUInt32(cityHash64(number % 2000)))) = sum(bitmapContains(materialize(bm), toUInt32(cityHash64(number % 2000)))),
    sum(bitmapContains(bm, toUInt32(cityHash64(number % 2000))))
FROM numbers(1048576);

-- Values beyond the bitmap range and beyond UInt32.
WITH (SELECT groupBitmapState(toUInt32(number * 3)) FROM numbers(100000)) AS bm
SELECT bitmapContains(bm, 299997), bitmapContains(bm, 300000), bitmapContains(bm, 4294967295), bitmapContains(bm, 4294967296), bitmapContains(bm, -1);

-- Small set (stored inline, not as a roaring bitmap).
WITH (SELECT groupBitmapState(toUInt32(number * 1000)) FROM numbers(10)) AS bm
SELECT sum(bitmapContains(bm, toUInt32(number))) FROM numbers(10000);

-- Signed and 64-bit bitmaps.
WITH (SELECT groupBitmapState(toInt32(number - 50000)) FROM numbers(100000)) AS bm
SELECT sum(bitmapContains(bm, toInt32(number - 60000))) FROM numbers(120000);

WITH (SELECT groupBitmapState(toUInt64(number * 4294967296)) FROM numbers(1000)) AS bm
SELECT sum(bitmapContains(bm, toUInt64(number * 2147483648))) FROM numbers(2000);

-- Non-constant bitmap column: one bitmap per row.
SELECT bitmapContains(bm, toUInt32(number)) FROM (SELECT number, groupBitmapState(toUInt32(number * 2)) AS bm FROM numbers(6) GROUP BY number) ORDER BY number;
