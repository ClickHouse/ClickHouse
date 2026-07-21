-- Tags: no-old-analyzer

-- Kind-scope boundaries of the band join and `join_use_nulls` behavior of the in-scope kinds.
-- Kinds that keep unmatched interval-side rows (RIGHT/FULL relative to the point side, and
-- SEMI/ANTI keeping the interval side) fall through to IEJoin listed next; so do the in-scope
-- non-INNER kinds when extra ON conjuncts remain (they affect matching, and residual
-- evaluation inside the band operator is not implemented).

-- Keep the written join order so the pins below see the orientation and kind as written.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,ie_join,hash';

DROP TABLE IF EXISTS kf_p;
DROP TABLE IF EXISTS kf_i;

CREATE TABLE kf_p (id UInt32, t Nullable(Int64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE kf_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

INSERT INTO kf_p SELECT number, if(number % 11 = 0, NULL, ((number * number + 6789) % 2147483647) % 60) FROM numbers(200);
INSERT INTO kf_i
    SELECT number, x, x + (number % 7) - 2
    FROM (SELECT number, (((number + 100) * (number + 100) + 12345) % 2147483647) % 40 AS x FROM numbers(200));

-- Kinds keeping unmatched interval-side rows decline and fall through to IEJoin
SELECT 'right of point-left', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_p p RIGHT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'left of point-right', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_i i LEFT JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'full point-left', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_p p FULL JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'full point-right', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_i i FULL JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'semi of interval side', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_p p RIGHT SEMI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi);
SELECT 'anti of interval side', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_i i LEFT ANTI JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi);

-- Extra ON conjuncts push down as a post-join filter only for ALL INNER: the non-INNER kinds
-- decline and fall through to IEJoin, which evaluates them as a residual condition inside
SELECT 'inner extra conjunct', count() > 0
FROM (EXPLAIN SELECT count() FROM kf_p p JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi AND p.id != i.id) WHERE explain LIKE '%BandJoin%';
SELECT 'left extra conjunct', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi AND p.id != i.id);
SELECT 'semi extra conjunct', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_p p LEFT SEMI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi AND p.id != i.id);
SELECT 'anti extra conjunct', countIf(explain LIKE '%IEJoin%') > 0, countIf(explain LIKE '%BandJoin%')
FROM (EXPLAIN SELECT count() FROM kf_i i RIGHT ANTI JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi AND p.id != i.id);

-- With `join_use_nulls = 1` the padded interval columns are NULL instead of defaults;
-- results stay byte-identical to `ie_join` under the same setting, in both orientations
SET join_algorithm = 'band_join,hash';
SELECT 'left use_nulls plan', count() > 0 FROM (EXPLAIN SELECT count() FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1) WHERE explain LIKE '%BandJoin%';
SELECT 'left use_nulls padded',
    (SELECT countIf(i.id IS NULL AND i.lo IS NULL AND i.hi IS NULL) FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT countIf(id NOT IN (SELECT p.id FROM kf_p p, kf_i i WHERE p.t >= i.lo AND p.t <= i.hi)) FROM kf_p);
SELECT 'left use_nulls parity',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1, join_algorithm = 'ie_join');
SELECT 'anti use_nulls parity',
    (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM kf_p p LEFT ANTI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((p.id, p.t, i.id, i.lo, i.hi))) FROM kf_p p LEFT ANTI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1, join_algorithm = 'ie_join');
SELECT 'semi use_nulls parity',
    (SELECT arraySort(groupArray((p.id, p.t))) FROM kf_p p LEFT SEMI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM kf_p p LEFT SEMI JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1, join_algorithm = 'ie_join');
SELECT 'right use_nulls parity',
    (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM kf_i i RIGHT JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((i.id, i.lo, i.hi, p.id, p.t))) FROM kf_i i RIGHT JOIN kf_p p ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1, join_algorithm = 'ie_join');

-- `join_use_nulls` does not change which rows are emitted, only the padding of the unmatched
SELECT 'use_nulls row parity',
    (SELECT arraySort(groupArray((p.id, p.t))) FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 1)
        = (SELECT arraySort(groupArray((p.id, p.t))) FROM kf_p p LEFT JOIN kf_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_use_nulls = 0);

DROP TABLE kf_p;
DROP TABLE kf_i;
