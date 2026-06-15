-- Tests the `hash_function_seed` setting for seedable hash functions (issue #63482).
-- Each statement sets the seed at the top level (the setting is read once when the function
-- is constructed for the query), which is the way the setting is meant to be used.

SELECT '-- default (seed = 0) is unchanged --';
-- The default of 0 must reproduce the historical, unseeded values.
SELECT xxHash32('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT xxHash64('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT xxh3('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT hex(xxh3_128('Hello, world!')) SETTINGS hash_function_seed = 0;
SELECT murmurHash2_32('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT murmurHash2_64('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT murmurHash3_32('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT murmurHash3_64('Hello, world!') SETTINGS hash_function_seed = 0;
SELECT hex(murmurHash3_128('Hello, world!')) SETTINGS hash_function_seed = 0;
SELECT wyHash64('Hello, world!') SETTINGS hash_function_seed = 0;

SELECT '-- not setting the seed equals seed = 0 --';
SELECT xxHash64('Hello, world!') = 17691043854468224118;

SELECT '-- xxHash matches the canonical xxHash test vectors with a seed --';
-- XXH32("test", seed=0) = 1042293711, XXH32("test", seed=1) = 4152504606
SELECT xxHash32('test') SETTINGS hash_function_seed = 0;
SELECT xxHash32('test') SETTINGS hash_function_seed = 1;

SELECT '-- a non-zero seed changes the result of every seedable function (1 = changed) --';
SELECT xxHash32('Hello, world!')       != 834093149            SETTINGS hash_function_seed = 12345;
SELECT xxHash64('Hello, world!')       != 17691043854468224118 SETTINGS hash_function_seed = 12345;
SELECT xxh3('Hello, world!')           != 17564966470555134057 SETTINGS hash_function_seed = 12345;
SELECT hex(xxh3_128('Hello, world!'))  != 'ADE2B2886B80787BBB5CA743B534F2FA' SETTINGS hash_function_seed = 12345;
SELECT murmurHash2_32('Hello, world!') != 1077681669           SETTINGS hash_function_seed = 12345;
SELECT murmurHash2_64('Hello, world!') != 11600739918808951577 SETTINGS hash_function_seed = 12345;
SELECT murmurHash3_32('Hello, world!') != 3224780355           SETTINGS hash_function_seed = 12345;
SELECT murmurHash3_64('Hello, world!') != 15952677324548579515 SETTINGS hash_function_seed = 12345;
SELECT hex(murmurHash3_128('Hello, world!')) != 'DF65D6D2D12D51F164C5F3A85066322C' SETTINGS hash_function_seed = 12345;
SELECT wyHash64('Hello, world!')       != 7490563016637708689  SETTINGS hash_function_seed = 12345;

SELECT '-- the same seed is deterministic across statements (1 = equal) --';
SELECT xxHash64('Hello, world!') SETTINGS hash_function_seed = 999;
SELECT xxHash64('Hello, world!') SETTINGS hash_function_seed = 999;

SELECT '-- the seed applies to non-String inputs too (1 = changed) --';
SELECT xxHash64(toUInt32(123)) != 18259948255902865962 SETTINGS hash_function_seed = 5;

SELECT '-- the seed applies per row, not just to constants (1 = changed) --';
SELECT sum(xxHash64(toString(number))) != 9447509538163294618 FROM numbers(100) SETTINGS hash_function_seed = 5;

SELECT '-- non-seedable functions ignore the setting (1 = unchanged) --';
SELECT cityHash64('Hello, world!')      = 2359500134450972198  SETTINGS hash_function_seed = 777;
SELECT farmHash64('Hello, world!')      = 1915388679533807205  SETTINGS hash_function_seed = 777;
SELECT halfMD5('Hello, world!')         = 7841705306765501771  SETTINGS hash_function_seed = 777;
SELECT gccMurmurHash('Hello, world!')   = 12518601801603912089 SETTINGS hash_function_seed = 777;
SELECT kafkaMurmurHash('Hello, world!') = 1052416786           SETTINGS hash_function_seed = 777;
SELECT sipHash64('Hello, world!')       = 12560575472004092239 SETTINGS hash_function_seed = 777;
SELECT metroHash64('Hello, world!')     = 676721872007707627   SETTINGS hash_function_seed = 777;
