-- Tags: no-old-analyzer

-- OUTER/SEMI/ANTI IEJoin with the default `join_use_nulls = 0`: unmatched rows are padded
-- with the default values of the other side's types (0 for numbers, '' for strings, NULL only
-- for columns that are Nullable themselves). The band condition `l.k > r.lo AND l.k < r.hi`
-- is crafted so that the match set equals that of the equality join `l.k = r.mid`
-- (lo = mid - 1, hi = mid + 1, and all keys are multiples of 10), so every result is
-- cross-checked against a hash join of the same kind.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET join_use_nulls = 0;

DROP TABLE IF EXISTS lpad;
DROP TABLE IF EXISTS rpad;

CREATE TABLE lpad (id Int32, k Nullable(Int32), s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE rpad (id Int32, lo Nullable(Int32), hi Nullable(Int32), mid Nullable(Int32), s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO lpad VALUES
    (1, 10, 'l1'),
    (2, 20, 'l2a'),
    (3, 20, 'l2b'),
    (4, 30, 'l3'),
    (5, 40, 'l4'),
    (6, NULL, 'lnull');

INSERT INTO rpad VALUES
    (1, 9, 11, 10, 'r1'),
    (2, 19, 21, 20, 'r2'),
    (3, 29, 31, 30, 'r3a'),
    (4, 29, 31, 30, 'r3b'),
    (5, 49, 51, 50, 'r5'),
    (6, NULL, NULL, NULL, 'rnull');

SELECT 'left';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l LEFT JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT JOIN rpad r ON l.k = r.mid);

SELECT 'right';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT JOIN rpad r ON l.k = r.mid);

SELECT 'full';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l FULL JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l FULL JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l FULL JOIN rpad r ON l.k = r.mid);

-- SEMI carries values from one arbitrary matching right row: project only the right columns
-- that are identical across a group's rows (lo, hi) so the result is deterministic
SELECT 'semi';
SELECT l.id, l.k, l.s, r.lo, r.hi FROM lpad l LEFT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.lo, r.hi))) FROM lpad l LEFT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.lo, r.hi))) FROM lpad l LEFT SEMI JOIN rpad r ON l.k = r.mid);

SELECT 'anti';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l LEFT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l LEFT ANTI JOIN rpad r ON l.k = r.mid);

SELECT 'right semi';
SELECT l.k, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.k, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT SEMI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.k, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT SEMI JOIN rpad r ON l.k = r.mid);

SELECT 'right anti';
SELECT l.id, l.k, l.s, r.id, r.lo, r.hi, r.s FROM lpad l RIGHT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi ORDER BY ALL;
SELECT (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT ANTI JOIN rpad r ON l.k > r.lo AND l.k < r.hi)
     = (SELECT arraySort(groupArray((l.id, l.k, l.s, r.id, r.lo, r.hi, r.s))) FROM lpad l RIGHT ANTI JOIN rpad r ON l.k = r.mid);

DROP TABLE lpad;
DROP TABLE rpad;
