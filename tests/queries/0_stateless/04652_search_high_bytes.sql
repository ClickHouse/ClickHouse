SET enable_analyzer = 1;
-- Substring search must find needle bytes above 0x7F. StringZilla's SWAR byte broadcast widened the
-- needle byte through a plain `char` before splatting it across a 64-bit word, so `sz_find_byte_serial`
-- and `sz_rfind_byte_serial` matched a byte in 0x80..0xFF only at word-relative offset 0.
--
-- The window where this is reachable is narrow, which is why it reproduced on some machines and not
-- others. Plain `char` is signed only on x86-64; the AArch64 ABI makes it unsigned, so the broadcast
-- there was always correct and the bug never existed. And on x86-64 the `skylake` and `icelake`
-- kernels handle their tails with masked AVX-512 loads instead of delegating to the serial kernel,
-- so with `SZ_DYNAMIC_DISPATCH` an AVX-512 machine never reaches the broken code either. Only the
-- `westmere` and `haswell` tiers hand short inputs and tails to the serial kernel, so the bug needed
-- x86-64 hardware below Skylake - which no CI runner is, hence `Bugfix validation` cannot catch it.
-- https://github.com/ClickHouse/ClickHouse/issues/111232
-- https://github.com/ashvardanian/StringZilla/issues/306

DROP TABLE IF EXISTS t_search_high_bytes;
CREATE TABLE t_search_high_bytes (v String) ENGINE = Memory;

-- More than two rows, so the vectorized code path is taken rather than the constant one.
INSERT INTO t_search_high_bytes SELECT unhex('78BE79') FROM numbers(8);

SELECT DISTINCT position(v, unhex('BE')), v LIKE unhex('25BE25') FROM t_search_high_bytes;

-- The same search over a constant.
SELECT position(unhex('78BE79'), unhex('BE'));

-- Every high byte must be found at every offset of every haystack length up to 70. The lengths
-- matter as much as the offsets: the broken kernel first goes wrong at length 8, where the SWAR
-- loop starts being used, and short haystacks are exactly what the SIMD kernels hand to it.
SELECT
    countIf(position(haystack, needle) != offset) AS position_mismatches,
    countIf(NOT (haystack LIKE concat('%', needle, '%'))) AS like_mismatches,
    countIf(countSubstrings(haystack, needle) != 1) AS count_mismatches
FROM
(
    SELECT
        char(number) AS needle,
        offset,
        concat(repeat('a', offset - 1), needle, repeat('z', len - offset)) AS haystack
    FROM numbers(128, 128)
    ARRAY JOIN range(1, 71) AS len
    ARRAY JOIN range(1, 71) AS offset
    WHERE offset <= len
);

DROP TABLE t_search_high_bytes;
