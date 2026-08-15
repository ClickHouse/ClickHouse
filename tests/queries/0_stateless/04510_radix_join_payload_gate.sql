-- Tags: no-random-settings
-- The `radix_join` algorithm physically radix-scatters the whole build side and every probe window,
-- so it requires ALL columns of both sides (not only the key columns) to be fixed-width. When a
-- projected payload column is String / Array / Nullable / LowCardinality on either side, the gate
-- rejects and the query falls back to `parallel_hash` (or plain `hash`). Either way the result must
-- match `hash`. When all projected columns are fixed-width, the radix path runs. This test pins that
-- payload-side gate; 04508 covers the key-side gate.
-- The old analyzer was intentionally not taught `radix_join`, hence enable_analyzer = 1.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS rhj_pg_b;
DROP TABLE IF EXISTS rhj_pg_p;

CREATE TABLE rhj_pg_b (k UInt64, f1 UInt64, f2 UInt32, s String, arr Array(UInt64), n Nullable(UInt64)) ENGINE = Memory;
CREATE TABLE rhj_pg_p (k UInt64, g1 UInt64, g2 UInt32, s String, arr Array(UInt64), n Nullable(UInt64)) ENGINE = Memory;

-- Overlapping but unequal key ranges with duplicate keys (many-to-many).
INSERT INTO rhj_pg_b SELECT
    number % 100,
    number,
    toUInt32(number),
    toString(number % 100),
    [number, number + 1],
    if(number % 7 = 0, NULL, number)
FROM numbers(300);
INSERT INTO rhj_pg_p SELECT
    number % 150,
    number,
    toUInt32(number),
    toString(number % 150),
    [number, number + 2],
    if(number % 5 = 0, NULL, number)
FROM numbers(200);

-- Accept shapes: only fixed-width columns projected -> the radix path runs and must equal hash.
SELECT 'accept_fixed_u64', (SELECT (count(), sum(cityHash64(p.g1, b.f1))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                          = (SELECT (count(), sum(cityHash64(p.g1, b.f1))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'accept_fixed_multi', (SELECT (count(), sum(cityHash64(p.g1, p.g2, b.f1, b.f2))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                            = (SELECT (count(), sum(cityHash64(p.g1, p.g2, b.f1, b.f2))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

-- Fallback shapes: a non-fixed-width payload projected on either side -> fall back, still equal hash.
SELECT 'fallback_right_string', (SELECT (count(), sum(cityHash64(p.g1, b.s))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                               = (SELECT (count(), sum(cityHash64(p.g1, b.s))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'fallback_left_string', (SELECT (count(), sum(cityHash64(p.s, b.f1))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                              = (SELECT (count(), sum(cityHash64(p.s, b.f1))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'fallback_right_array', (SELECT (count(), sum(cityHash64(p.g1, arrayStringConcat(arrayMap(x -> toString(x), b.arr), ',')))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                              = (SELECT (count(), sum(cityHash64(p.g1, arrayStringConcat(arrayMap(x -> toString(x), b.arr), ',')))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'fallback_right_nullable', (SELECT (count(), sum(cityHash64(p.g1, toString(b.n)))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join')
                                 = (SELECT (count(), sum(cityHash64(p.g1, toString(b.n)))) FROM rhj_pg_p AS p INNER JOIN rhj_pg_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

DROP TABLE rhj_pg_b;
DROP TABLE rhj_pg_p;
