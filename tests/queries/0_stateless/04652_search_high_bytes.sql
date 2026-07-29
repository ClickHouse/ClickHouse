SET enable_analyzer = 1;
-- Substring search must find needle bytes above 0x7F. StringZilla's SWAR byte broadcast
-- sign-extended the needle byte before splatting it across a 64-bit word, so `sz_find_byte_serial`
-- never matched a byte in 0x80..0xFF. The `westmere`, `haswell`, `neon` and `sve` kernels all
-- delegate short inputs and their tails to that serial code, so only the AVX-512 tiers were
-- unaffected - which is why this reproduced on some machines and not others.
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
