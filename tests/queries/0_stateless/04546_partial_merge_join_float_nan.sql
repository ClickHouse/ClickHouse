-- Test compareTrackAt with Float64 / +-Inf / NaN keys in partial_merge join.
-- ColumnVector<Float64>::compareTrackAt (ColumnVector.h:157) uses
-- CompareHelper<Float64>::compare, which drives multi-row skipping over the
-- sorted Float keys. To actually exercise the NaN branch of compareTrackAt the
-- NaN key is present on BOTH sides as the first key; a mismatching second key
-- then keeps the overall join from matching. partial_merge compares float keys
-- bitwise (nullableCompareAt), so NaN==NaN on the first key; the second key is
-- what excludes the pair. This is independent of the equi-join NaN semantics
-- change in the hash / full_sorting_merge / direct algorithms (PR #106540),
-- which explicitly leaves partial_merge's bitwise behavior unchanged.

SET join_algorithm = 'partial_merge';
-- Pin max_block_size so the whole sorted left input is one block. CI randomizes
-- max_block_size; a small value would split the 7,8,9 < 10 run across blocks and
-- compareTrackAt would never return |track| > 1, silently voiding the coverage.
SET max_block_size = 65505;

DROP TABLE IF EXISTS t_04546_left;
DROP TABLE IF EXISTS t_04546_right;

CREATE TABLE t_04546_left  (k1 Float64, k2 UInt32, val String) ENGINE = MergeTree() ORDER BY (k1, k2);
CREATE TABLE t_04546_right (k1 Float64, k2 UInt32, val String) ENGINE = MergeTree() ORDER BY (k1, k2);

-- Left  k1:  -inf, 1, 2, 3, 7, 8, 9, inf, nan, nan
-- Right k1:   0, 3, 5, 10, inf, nan
-- Finite match: 3. +-Inf: +inf matches, -inf does not. Runs 7,8,9 < 10 force a
-- first-key track skip. NaN is on both sides so compareTrackAt compares NaN vs
-- NaN, but the second key differs (10/20 left vs 30 right) so no NaN pair joins.
INSERT INTO t_04546_left  VALUES (-inf, 0, 'L-inf'), (1, 0, 'L1'), (2, 0, 'L2'), (3, 0, 'L3'), (7, 0, 'L7'), (8, 0, 'L8'), (9, 0, 'L9'), (inf, 0, 'Linf'), (nan, 10, 'Lnan10'), (nan, 20, 'Lnan20');
INSERT INTO t_04546_right VALUES (0, 0, 'R0'), (3, 0, 'R3'), (5, 0, 'R5'), (10, 0, 'R10'), (inf, 0, 'Rinf'), (nan, 30, 'Rnan30');

-- INNER JOIN on (k1, k2): matches are (3,0) and (inf,0); no NaN pair matches.
-- isNaN() prefix pins any NaN row last so the ORDER BY is deterministic
-- regardless of the sort's nan_direction_hint (which CI randomizes).
SELECT l.k1, l.k2, l.val, r.val
FROM t_04546_left l INNER JOIN t_04546_right r ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY isNaN(l.k1), l.k1, l.k2, l.val;

-- LEFT JOIN: all left rows, right values for matches (3,0) and (inf,0); NaN rows unmatched.
SELECT l.k1, l.k2, l.val, r.val
FROM t_04546_left l LEFT JOIN t_04546_right r ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY isNaN(l.k1), l.k1, l.k2, l.val;

-- compareTrackAt sees NaN vs NaN on k1, but the second key differs, so no NaN row joins (expect 0).
SELECT count() FROM t_04546_left l INNER JOIN t_04546_right r ON l.k1 = r.k1 AND l.k2 = r.k2 WHERE isNaN(l.k1);

DROP TABLE t_04546_left;
DROP TABLE t_04546_right;
