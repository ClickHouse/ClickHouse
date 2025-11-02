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
