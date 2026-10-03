-- Tags: no-old-analyzer

-- Point-side LEFT / SEMI / ANTI band joins in the swapped orientation: the interval table is
-- written on the left and the point expression comes from the query's right table, so RIGHT /
-- RIGHT SEMI / RIGHT ANTI keep the point side and execute as the LEFT mirror with swapped
-- input pipelines (`Swapped: true`, `PointSide: Right` in EXPLAIN) and the query column order
-- restored on top. Verified against a brute-force cross-join oracle and byte-for-byte against
-- `ie_join`; SEMI compares the point-side columns only (the emitted interval row is not fixed
-- across algorithms).

-- Pin the written join order: the join order optimizer may flip the inputs and the kind on
-- its own, which would change what reaches the band join planner code.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS ks_p;
DROP TABLE IF EXISTS ks_i;

CREATE TABLE ks_p (id UInt32, t Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ks_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

-- The point domain (60) is wider than the interval domain (40), so a good share of the point
-- rows is unmatched; every 37th point key is NULL; interval widths from -2 (empty) to 4.
INSERT INTO ks_p SELECT number, if(number % 37 = 0, NULL, ((number * number + 6789) % 2147483647) % 60) FROM numbers(400);
INSERT INTO ks_i
    SELECT number, x, x + (number % 7) - 2
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(400));

-- The executed type is the LEFT mirror of the query kind, with the swap recorded honestly
SELECT 'right type', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT%';
SELECT 'right swapped', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Swapped: true%';
SELECT 'right point side', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%PointSide: Right%';
SELECT 'semi type', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT SEMI%';
SELECT 'semi swapped', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Swapped: true%';
SELECT 'anti type', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%Type: LEFT ANTI%';

-- RIGHT: every point row is kept; unmatched ones carry the interval columns padded with
-- defaults; the restored column order puts the interval table's columns first
SELECT 'right >= <=' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((i.id, i.lo, i.hi, p.id, p.t)) FROM ks_i i, ks_p p WHERE p.t >= i.lo AND p.t <= i.hi),
            (SELECT groupArray((toUInt32(0), toInt64(0), toInt64(0), id, t)) FROM ks_p WHERE id NOT IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t >= i.lo AND p.t <= i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'right > <' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((i.id, i.lo, i.hi, p.id, p.t)) FROM ks_i i, ks_p p WHERE p.t > i.lo AND p.t < i.hi),
            (SELECT groupArray((toUInt32(0), toInt64(0), toInt64(0), id, t)) FROM ks_p WHERE id NOT IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t > i.lo AND p.t < i.hi))))) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT JOIN ks_p p ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ks_p WHERE id IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t >= i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((id, t))) FROM ks_p WHERE id IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t > i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti >= <=' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((toUInt32(0), toInt64(0), toInt64(0), id, t))) FROM ks_p WHERE id NOT IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t >= i.lo AND p.t <= i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti > <' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((toUInt32(0), toInt64(0), toInt64(0), id, t))) FROM ks_p WHERE id NOT IN (SELECT p.id FROM ks_i i, ks_p p WHERE p.t > i.lo AND p.t < i.hi)) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t > i.lo AND p.t < i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity;

-- Both orientations of every kind produce the same point-side result set
SELECT 'orientation parity left',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ks_i i RIGHT JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM ks_p p LEFT JOIN ks_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'orientation parity semi',
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT SEMI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_p p LEFT SEMI JOIN ks_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'orientation parity anti',
    (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_i i RIGHT ANTI JOIN ks_p p ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM ks_p p LEFT ANTI JOIN ks_i i ON p.t >= i.lo AND p.t <= i.hi);

DROP TABLE ks_p;
DROP TABLE ks_i;
