-- Tags: no-random-settings
-- The `radix_join` join algorithm must produce results identical to `hash` for every join shape:
-- when its gate applies (a single-disjunct INNER ALL equi-join over fixed-width, non-nullable,
-- non-LowCardinality keys whose packed width is a multiple of 4 in [4, 64]) it runs the radix path,
-- and otherwise (String / LowCardinality / Nullable / sub-4B or non-multiple-of-4 or >64B keys, a
-- non-INNER kind, a non-ALL strictness, an OR-disjunct ON) it falls back to `parallel_hash` (or plain
-- `hash` where even that shape does not hold). Either way the output must match `hash`.
-- The old analyzer was intentionally not taught `radix_join`, hence enable_analyzer = 1.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS rhj_b;
DROP TABLE IF EXISTS rhj_p;

CREATE TABLE rhj_b (a UInt64, b UInt32, c UInt8, s String, lc LowCardinality(String), n Nullable(UInt64), d Date32, u UUID, f16 FixedString(16), f68 FixedString(68), f3 FixedString(3), k1 UInt64, k2 UInt64, k3 UInt64, k4 UInt64, k5 UInt64, k6 UInt64, k7 UInt64, k8 UInt64, pay UInt64) ENGINE = Memory;
CREATE TABLE rhj_p (a UInt64, b UInt32, c UInt8, s String, lc LowCardinality(String), n Nullable(UInt64), d Date32, u UUID, f16 FixedString(16), f68 FixedString(68), f3 FixedString(3), k1 UInt64, k2 UInt64, k3 UInt64, k4 UInt64, k5 UInt64, k6 UInt64, k7 UInt64, k8 UInt64, pay UInt64) ENGINE = Memory;

-- Overlapping but unequal key ranges on the two sides, with duplicate keys (many-to-many). All key
-- columns derive from one base key so every join shape below has the same match structure.
INSERT INTO rhj_b SELECT
    number % 100,
    toUInt32(number % 100),
    toUInt8(number % 100),
    toString(number % 100),
    toString(number % 100),
    if(number % 7 = 0, NULL, number % 100),
    toDate32('2020-01-01') + (number % 100),
    reinterpretAsUUID(toFixedString(leftPad(toString(number % 100), 16, 'x'), 16)),
    toFixedString(leftPad(toString(number % 100), 16, 'y'), 16),
    toFixedString(leftPad(toString(number % 100), 68, 'z'), 68),
    toFixedString(leftPad(toString(number % 9), 3, 'w'), 3),
    number % 100, number % 100 + 1, number % 100 + 2, number % 100 + 3,
    number % 100 + 4, number % 100 + 5, number % 100 + 6, number % 100 + 7,
    number
FROM numbers(300);
INSERT INTO rhj_p SELECT
    number % 150,
    toUInt32(number % 150),
    toUInt8(number % 150),
    toString(number % 150),
    toString(number % 150),
    if(number % 5 = 0, NULL, number % 150),
    toDate32('2020-01-01') + (number % 150),
    reinterpretAsUUID(toFixedString(leftPad(toString(number % 150), 16, 'x'), 16)),
    toFixedString(leftPad(toString(number % 150), 16, 'y'), 16),
    toFixedString(leftPad(toString(number % 150), 68, 'z'), 68),
    toFixedString(leftPad(toString(number % 13), 3, 'w'), 3),
    number % 150, number % 150 + 1, number % 150 + 2, number % 150 + 3,
    number % 150 + 4, number % 150 + 5, number % 150 + 6, number % 150 + 7,
    number
FROM numbers(200);

-- Each row prints the case name and 1 when radix_join agrees with hash on (count, value fingerprint).

-- Gate ACCEPT shapes (the radix path itself).
SELECT 'single_u64', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'radix_join')
                   = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'hash');

SELECT 'single_u32', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.b = b.b SETTINGS join_algorithm = 'radix_join')
                   = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.b = b.b SETTINGS join_algorithm = 'hash');

SELECT 'composite_u64_u32', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a AND p.b = b.b SETTINGS join_algorithm = 'radix_join')
                          = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a AND p.b = b.b SETTINGS join_algorithm = 'hash');

SELECT 'single_date32', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.d = b.d SETTINGS join_algorithm = 'radix_join')
                      = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.d = b.d SETTINGS join_algorithm = 'hash');

SELECT 'single_uuid', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.u = b.u SETTINGS join_algorithm = 'radix_join')
                    = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.u = b.u SETTINGS join_algorithm = 'hash');

SELECT 'single_fixedstring16', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f16 = b.f16 SETTINGS join_algorithm = 'radix_join')
                             = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f16 = b.f16 SETTINGS join_algorithm = 'hash');

SELECT 'composite_64byte_u64x8', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.k1 = b.k1 AND p.k2 = b.k2 AND p.k3 = b.k3 AND p.k4 = b.k4 AND p.k5 = b.k5 AND p.k6 = b.k6 AND p.k7 = b.k7 AND p.k8 = b.k8 SETTINGS join_algorithm = 'radix_join')
                               = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.k1 = b.k1 AND p.k2 = b.k2 AND p.k3 = b.k3 AND p.k4 = b.k4 AND p.k5 = b.k5 AND p.k6 = b.k6 AND p.k7 = b.k7 AND p.k8 = b.k8 SETTINGS join_algorithm = 'hash');

-- Gate REJECT shapes (the fallback path; results must still equal hash).
SELECT 'fallback_u8', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.c = b.c SETTINGS join_algorithm = 'radix_join')
                    = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.c = b.c SETTINGS join_algorithm = 'hash');

SELECT 'fallback_string', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.s = b.s SETTINGS join_algorithm = 'radix_join')
                        = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.s = b.s SETTINGS join_algorithm = 'hash');

SELECT 'fallback_lowcard', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.lc = b.lc SETTINGS join_algorithm = 'radix_join')
                         = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.lc = b.lc SETTINGS join_algorithm = 'hash');

SELECT 'fallback_nullable', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.n = b.n SETTINGS join_algorithm = 'radix_join')
                          = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.n = b.n SETTINGS join_algorithm = 'hash');

SELECT 'fallback_composite_nm4', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a AND p.c = b.c SETTINGS join_algorithm = 'radix_join')
                               = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a AND p.c = b.c SETTINGS join_algorithm = 'hash');

SELECT 'fallback_fixedstring68', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f68 = b.f68 SETTINGS join_algorithm = 'radix_join')
                               = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f68 = b.f68 SETTINGS join_algorithm = 'hash');

SELECT 'fallback_fixedstring3', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f3 = b.f3 SETTINGS join_algorithm = 'radix_join')
                              = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.f3 = b.f3 SETTINGS join_algorithm = 'hash');

SELECT 'fallback_left_join', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p LEFT JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'radix_join')
                           = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p LEFT JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'hash');

-- INNER ANY picks ONE arbitrary matching build row per probe row, so the fingerprint may only use
-- columns functionally determined by the key (b.a, not b.pay) to be algorithm-independent.
SELECT 'fallback_inner_any', (SELECT (count(), sum(cityHash64(p.pay, b.a))) FROM rhj_p AS p INNER ANY JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'radix_join')
                           = (SELECT (count(), sum(cityHash64(p.pay, b.a))) FROM rhj_p AS p INNER ANY JOIN rhj_b AS b ON p.a = b.a SETTINGS join_algorithm = 'hash');

-- An OR-disjunct ON clause: `chooseJoinAlgorithm` requires `hash` (or `auto`) to be enabled for
-- multi-disjunct joins before any per-algorithm gate runs, so `radix_join` is listed together with
-- `hash`; the radix gate then rejects (oneDisjunct fails) and the fallback produces the plain hash join.
SELECT 'fallback_or_disjunct', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a OR p.b = b.b SETTINGS join_algorithm = 'radix_join,hash')
                             = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_p AS p INNER JOIN rhj_b AS b ON p.a = b.a OR p.b = b.b SETTINGS join_algorithm = 'hash');

DROP TABLE rhj_b;
DROP TABLE rhj_p;
