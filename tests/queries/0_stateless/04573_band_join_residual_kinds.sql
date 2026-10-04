-- Tags: no-old-analyzer

-- Residual ON conjuncts inside the band join for the non-INNER kinds: the extra conjunct
-- `p.v < i.w` over Nullable columns gates the candidates inside the probe (a NULL result
-- counts as failed), LEFT rows are unmatched iff no candidate passed, SEMI/ANTI are decided
-- by the first passing candidate. Verified against the cross-join oracle (WHERE folds NULL
-- to failed the same way) and byte-for-byte against `ie_join` on the same queries; SEMI
-- compares the point-side columns only.

-- Keep the written join order so the checks below exercise the orientation as written.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS br_p;
DROP TABLE IF EXISTS br_i;

CREATE TABLE br_p (id UInt32, t Nullable(Int64), v Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE br_i (id UInt32, lo Int64, hi Int64, w Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

-- The point domain (60) is wider than the interval domain (40); every 37th point key and
-- every 5th residual operand is NULL on the point side, every 7th on the interval side.
INSERT INTO br_p SELECT
    number,
    if(number % 37 = 0, NULL, ((number * number + 6789) % 2147483647) % 60),
    if(number % 5 = 0, NULL, toInt32(number % 13))
FROM numbers(400);
INSERT INTO br_i
    SELECT number, x, x + (number % 7) - 2, if(number % 7 = 0, NULL, toInt32((number + 4) % 13))
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(400));

-- The non-INNER kinds with an extra conjunct run as a band join with an in-operator residual
SELECT 'left plan', countIf(explain LIKE '%BandJoin%') > 0, countIf(explain LIKE '%Residual filter%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w);
SELECT 'semi plan', countIf(explain LIKE '%BandJoin%') > 0, countIf(explain LIKE '%Residual filter%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM br_p p LEFT SEMI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w);
SELECT 'anti plan', countIf(explain LIKE '%BandJoin%') > 0, countIf(explain LIKE '%Residual filter%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM br_p p LEFT ANTI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w);
SELECT 'swapped plan', countIf(explain LIKE '%Residual filter%') > 0, countIf(explain LIKE '%Swapped: true%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM br_i i RIGHT JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w);

SELECT 'left >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w)) FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w),
            (SELECT groupArray((id, t, v, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'left > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w)) FROM br_p p, br_i i WHERE p.t > i.lo AND p.t < i.hi AND p.v < i.w),
            (SELECT groupArray((id, t, v, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t > i.lo AND p.t < i.hi AND p.v < i.w))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((id, t, v))) FROM br_p WHERE id IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'semi > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((id, t, v))) FROM br_p WHERE id IN (SELECT p.id FROM br_p p, br_i i WHERE p.t > i.lo AND p.t < i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_p p LEFT SEMI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((id, t, v, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)')))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'anti > <' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((id, t, v, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)')))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t > i.lo AND p.t < i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT ANTI JOIN br_i i ON p.t > i.lo AND p.t < i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

-- The swapped orientation (point side on the right) flips the residual's source sides
SELECT 'right >= <=' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v)) FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w),
            (SELECT groupArray((toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'), id, t, v)) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w))))) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'right semi >= <=' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_i i RIGHT SEMI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((id, t, v))) FROM br_p WHERE id IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_i i RIGHT SEMI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v))) FROM br_i i RIGHT SEMI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

SELECT 'right anti >= <=' AS q,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT ANTI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'), id, t, v))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)) AS oracle_ok,
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT ANTI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, i.w, p.id, p.t, p.v))) FROM br_i i RIGHT ANTI JOIN br_p p ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_algorithm = 'ie_join') AS ie_parity;

-- A residual over point-side columns only: LEFT still pads the rows it fails
SELECT 'point-only residual' AS q,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v > 6)
        = (SELECT arraySort(arrayConcat(
            (SELECT groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w)) FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v > 6),
            (SELECT groupArray((id, t, v, toUInt32(0), toInt64(0), toInt64(0), CAST(NULL, 'Nullable(Int32)'))) FROM br_p WHERE id NOT IN (SELECT p.id FROM br_p p, br_i i WHERE p.t >= i.lo AND p.t <= i.hi AND p.v > 6))))) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v > 6)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v > 6 SETTINGS join_algorithm = 'ie_join') AS ie_parity;

-- `join_use_nulls` with a residual stays byte-identical to `ie_join`
SELECT 'left use_nulls parity',
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS join_use_nulls = 1, join_algorithm = 'ie_join');

-- The output caps do not change the result with a residual (mid-row splits interleave flushes)
SELECT 'left caps',
    (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w SETTINGS max_joined_block_size_rows = 7)
        = (SELECT arraySort(groupArray((p.id, p.t, p.v, i.id, i.lo, i.hi, i.w))) FROM br_p p LEFT JOIN br_i i ON p.t >= i.lo AND p.t <= i.hi AND p.v < i.w);

DROP TABLE br_p;
DROP TABLE br_i;
