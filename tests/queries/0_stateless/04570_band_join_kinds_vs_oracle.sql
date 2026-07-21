-- Tags: no-old-analyzer

-- Point-side LEFT / SEMI / ANTI band joins on randomized duplicate-heavy data, for all four
-- strict/loose bracket combinations: verified against a brute-force cross-join oracle (matched
-- pairs from a comma join, unmatched rows reconstructed with default padding) and
-- byte-for-byte against `ie_join` on the same queries. SEMI compares the point-side columns
-- only: which matched interval row it emits is not fixed across algorithms.

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ko_p;
DROP TABLE IF EXISTS ko_i;
DROP TABLE IF EXISTS ko_empty;

CREATE TABLE ko_p (id UInt32, t Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ko_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ko_empty (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

-- The point domain (60) is wider than the interval domain (40), so a good share of the point
-- rows is unmatched; every 37th point key is NULL; interval widths from -2 (empty) to 4.
INSERT INTO ko_p SELECT number, if(number % 37 = 0, NULL, ((number * number + 6789) % 2147483647) % 60) FROM numbers(400);
INSERT INTO ko_i
    SELECT number, x, x + (number % 7) - 2
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(400));

-- The in-scope kinds go through the band join and EXPLAIN shows the executed type
SELECT 'left plan', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT%';
SELECT 'semi plan', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT SEMI%';
SELECT 'anti plan', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT ANTI%';

SELECT 'left >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, i.id, i.lo, i.hi)) FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t <= i.hi),
            (SELECT groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t <= i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'left > <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, i.id, i.lo, i.hi)) FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t <= i.hi),
            (SELECT groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t <= i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'left >= <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, i.id, i.lo, i.hi)) FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t < i.hi),
            (SELECT groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t < i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'left > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, i.id, i.lo, i.hi)) FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t < i.hi),
            (SELECT groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t < i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ko_p WHERE id IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi > <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ko_p WHERE id IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi >= <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ko_p WHERE id IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ko_p WHERE id IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0)))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti > <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0)))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti >= <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0)))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t >= i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((id, t, toUInt32(0), toInt64(0), toInt64(0)))) FROM ko_p WHERE id NOT IN (SELECT p.id FROM ko_p p, ko_i i WHERE p.t > i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

-- NULL-keyed point rows match nothing: LEFT and ANTI emit them padded, SEMI drops them
SELECT 'null keys',
    (SELECT countIf(t IS NULL) FROM ko_p) AS nulls,
    (SELECT countIf(p.t IS NULL AND i.id = 0 AND i.lo = 0 AND i.hi = 0) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) AS left_padded,
    (SELECT countIf(p.t IS NULL) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) AS anti_emitted,
    (SELECT countIf(p.t IS NULL) FROM ko_p p LEFT SEMI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi) AS semi_emitted;

-- An empty interval side: LEFT and ANTI emit every point row padded, SEMI emits nothing
SELECT 'empty left', count(), countIf(i.id = 0 AND i.lo = 0 AND i.hi = 0) FROM ko_p p LEFT JOIN ko_empty i ON p.t >= i.lo AND p.t <= i.hi;
SELECT 'empty anti', count() FROM ko_p p LEFT ANTI JOIN ko_empty i ON p.t >= i.lo AND p.t <= i.hi;
SELECT 'empty semi', count() FROM ko_p p LEFT SEMI JOIN ko_empty i ON p.t >= i.lo AND p.t <= i.hi;

-- The output caps split chunks with padded and matched runs interleaved
SELECT 'left caps',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS max_joined_block_size_rows = 7)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'anti caps',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS max_joined_block_size_rows = 3)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ko_p p LEFT ANTI JOIN ko_i i ON p.t >= i.lo AND p.t <= i.hi);

DROP TABLE ko_p;
DROP TABLE ko_i;
DROP TABLE ko_empty;
