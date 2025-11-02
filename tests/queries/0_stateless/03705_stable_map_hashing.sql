SELECT '-- Test 1: Default behavior (setting disabled)';
SET enable_stable_map_hashing = 0;

WITH
    materialize(map('aa', 1, 'bb', 2)) AS map_a,
    materialize(map('bb', 2, 'aa', 1)) AS map_b
SELECT
    cityHash64(map_a) = cityHash64(map_b) AS hashes_equal_city,
    sipHash64(map_a) = sipHash64(map_b) AS hashes_equal_sip,
    xxHash64(map_a) = xxHash64(map_b) AS hashes_equal_xx,
    murmurHash3_64(map_a) = murmurHash3_64(map_b) AS hashes_equal_murmur;

SELECT '-- Test 2: Stable hashing enabled';
SET enable_stable_map_hashing = 1;

WITH
    materialize(map('aa', 1, 'bb', 2)) AS map_a,
    materialize(map('bb', 2, 'aa', 1)) AS map_b
SELECT
    cityHash64(map_a) = cityHash64(map_b) AS hashes_equal_city,
    sipHash64(map_a) = sipHash64(map_b) AS hashes_equal_sip,
    xxHash64(map_a) = xxHash64(map_b) AS hashes_equal_xx,
    murmurHash3_64(map_a) = murmurHash3_64(map_b) AS hashes_equal_murmur;

SELECT '-- Test 3: Maps with duplicate keys (different value order)';
WITH
    materialize(map('aaa', 1, 'aaa', 2)) AS map_a,
    materialize(map('aaa', 2, 'aaa', 1)) AS map_b
SELECT
    cityHash64(map_a) = cityHash64(map_b) AS hashes_equal,
    sipHash64(map_a) = sipHash64(map_b) AS hashes_equal_sip,
    xxHash64(map_a) = xxHash64(map_b) AS hashes_equal_xx,
    murmurHash3_64(map_a) = murmurHash3_64(map_b) AS hashes_equal_murmur;

SELECT '-- Test 4: Maps with duplicate keys and multiple keys';
WITH
    materialize(map('x', 1, 'x', 2, 'y', 3)) AS map_a,
    materialize(map('y', 3, 'x', 2, 'x', 1)) AS map_b
SELECT
    cityHash64(map_a) = cityHash64(map_b) AS hashes_equal,
    sipHash64(map_a) = sipHash64(map_b) AS hashes_equal_sip,
    xxHash64(map_a) = xxHash64(map_b) AS hashes_equal_xx,
    murmurHash3_64(map_a) = murmurHash3_64(map_b) AS hashes_equal_murmur;
