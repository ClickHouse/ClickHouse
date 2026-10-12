-- Tags: no-old-analyzer

-- SEMI/ANTI IEJoin with a residual ON condition: the first candidate PASSING the residual
-- decides each driving-side row. High-fanout rows (thousands of candidates per row, with the
-- residual passing for one late candidate, for none, or yielding NULL for all) exercise the
-- bounded mini-batch evaluation inside the operator.

-- `ie_join` goes first: the ON section has an equality, so listed last IEJoin would never
-- claim these joins.
SET join_algorithm = 'ie_join,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS tsl;
DROP TABLE IF EXISTS tsr;

CREATE TABLE tsl (id UInt32, lo Int32, hi Int32, sel Nullable(Int32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tsr (id UInt32, v Int32, w Int32, tag Nullable(Int32)) ENGINE = MergeTree ORDER BY id;

-- Every left row's band covers all 3000 right rows (> 2 mini-batches of candidates);
-- the residual `tag = sel` decides:
--   id 1: passes for exactly one right row (tag 42 appears once, at the end of the value range)
--   id 2: passes for many right rows
--   id 3: passes for none (no tag -5)
--   id 4: sel is NULL, the residual is NULL for every candidate
--   id 5: a narrow band with a pass
INSERT INTO tsl VALUES (1, -1, 100000, 42), (2, -1, 100000, 7), (3, -1, 100000, -5), (4, -1, 100000, NULL), (5, 100, 110, 8);

INSERT INTO tsr SELECT number + 1, toInt32(number), toInt32(-1 - number), if(number = 2999, 42, if(number % 8 = 0, NULL, toInt32(number % 10))) FROM numbers(3000);

-- The joins must run as IEJoin with the equality as an in-operator residual
SELECT 'routed', count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM tsl l LEFT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag) WHERE explain LIKE '%IEJoin%';
SELECT 'residual', count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM tsl l LEFT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag) WHERE explain LIKE '%Residual filter%';
SELECT 'routed right', count() > 0 FROM (EXPLAIN actions = 1 SELECT r.id FROM tsl l RIGHT ANTI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag) WHERE explain LIKE '%IEJoin%';

SELECT 'semi', l.id FROM tsl l LEFT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag ORDER BY ALL;
SELECT 'anti', l.id FROM tsl l LEFT ANTI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag ORDER BY ALL;

-- The oracle from the cross join agrees on the decided rows
SELECT 'semi vs oracle', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tsl l LEFT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag)
) = (
    SELECT arraySort(groupArray(id)) FROM tsl WHERE id IN (SELECT l.id FROM tsl l, tsr r WHERE l.lo < r.v AND l.hi > r.w AND l.sel = r.tag)
);
SELECT 'anti vs oracle', (
    SELECT arraySort(groupArray(id)) FROM (SELECT l.id AS id FROM tsl l LEFT ANTI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag)
) = (
    SELECT arraySort(groupArray(id)) FROM tsl WHERE id NOT IN (SELECT l.id FROM tsl l, tsr r WHERE l.lo < r.v AND l.hi > r.w AND l.sel = r.tag)
);

-- The SEMI row's right-side companion must itself satisfy all conditions (which right row
-- is picked is not fixed, so project a condition check instead of values)
SELECT 'semi pair valid', l.id, (l.lo < r.v AND l.hi > r.w AND l.sel = r.tag)
FROM tsl l LEFT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag ORDER BY ALL;

-- RIGHT SEMI/ANTI run as the swapped left-side mirror with the residual's sides flipped:
-- right rows are decided by `tag = sel` against the single fat left band
SELECT 'right semi count', count() FROM tsl l RIGHT SEMI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag;
SELECT 'right semi oracle', count() FROM tsr WHERE id IN (SELECT r.id FROM tsl l, tsr r WHERE l.lo < r.v AND l.hi > r.w AND l.sel = r.tag);
SELECT 'right anti count', count() FROM tsl l RIGHT ANTI JOIN tsr r ON l.lo < r.v AND l.hi > r.w AND l.sel = r.tag;
SELECT 'right anti oracle', count() FROM tsr WHERE id NOT IN (SELECT r.id FROM tsl l, tsr r WHERE l.lo < r.v AND l.hi > r.w AND l.sel = r.tag);

DROP TABLE tsl;
DROP TABLE tsr;
