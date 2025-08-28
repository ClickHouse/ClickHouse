-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test hash functions
SELECT '=== Hash functions ===';

SELECT 
    cityHash64(toDecimal512('123.456', 3)) AS city_hash,
    xxHash64(toDecimal512('123.456', 3)) AS xx_hash,
    murmurHash2_64(toDecimal512('123.456', 3)) AS murmur_hash;

-- Test with different values
SELECT 
    cityHash64(toDecimal512('789.123', 3)) AS city_hash_different,
    xxHash64(toDecimal512('789.123', 3)) AS xx_hash_different,
    murmurHash2_64(toDecimal512('789.123', 3)) AS murmur_hash_different;

-- Test with zero
SELECT 
    cityHash64(toDecimal512('0.0', 3)) AS city_hash_zero,
    xxHash64(toDecimal512('0.0', 3)) AS xx_hash_zero,
    murmurHash2_64(toDecimal512('0.0', 3)) AS murmur_hash_zero;

-- Test with negative numbers
SELECT 
    cityHash64(toDecimal512('-123.456', 3)) AS city_hash_negative,
    xxHash64(toDecimal512('-123.456', 3)) AS xx_hash_negative,
    murmurHash2_64(toDecimal512('-123.456', 3)) AS murmur_hash_negative;

-- Test with different scales
SELECT 
    cityHash64(toDecimal512('123.456', 6)) AS city_hash_scale_6,
    cityHash64(toDecimal512('123.456', 3)) AS city_hash_scale_3,
    cityHash64(toDecimal512('123.456', 0)) AS city_hash_scale_0;

-- Test with very large numbers
SELECT 
    cityHash64(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS city_hash_large,
    xxHash64(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS xx_hash_large,
    murmurHash2_64(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS murmur_hash_large;

-- Test hash consistency (same value should produce same hash)
SELECT 
    cityHash64(toDecimal512('123.456', 3)) AS hash1,
    cityHash64(toDecimal512('123.456', 3)) AS hash2,
    cityHash64(toDecimal512('123.456', 3)) = cityHash64(toDecimal512('123.456', 3)) AS hash_consistency;

-- Test hash distribution (different values should produce different hashes)
SELECT 
    cityHash64(toDecimal512('123.456', 3)) AS hash_value1,
    cityHash64(toDecimal512('123.457', 3)) AS hash_value2,
    cityHash64(toDecimal512('123.456', 3)) != cityHash64(toDecimal512('123.457', 3)) AS hash_different;

