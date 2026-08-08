-- The right side is finalized on the build-phase-finish hook rather than as a side effect of
-- setTotals(), so a right side carrying WITH TOTALS must join exactly like one that does not,
-- and must agree with the hash algorithm. Before the fix a starved totals port left the right
-- side unmerged: RIGHT/FULL crashed on a null RowBitmaps and LEFT/INNER silently lost matches.

DROP TABLE IF EXISTS t04821_l;
DROP TABLE IF EXISTS t04821_r;

CREATE TABLE t04821_l (k UInt64, lv String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t04821_r (k UInt64, rv String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t04821_l SELECT number, concat('l', toString(number)) FROM numbers(8);
INSERT INTO t04821_r SELECT number, concat('r', toString(number)) FROM numbers(8);

SET join_use_nulls = 0;

-- Every block below asserts that `partial_merge` agrees with `hash` on the same query.
-- countIf(rv != '') is the wrong-results detector: an unmerged right side yields 0.

SELECT '-- inner: partial_merge vs hash';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l INNER JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l INNER JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

SELECT '-- left: partial_merge vs hash';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l LEFT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l LEFT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

SELECT '-- right: partial_merge vs hash';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l RIGHT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l RIGHT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

SELECT '-- full: partial_merge vs hash';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

-- Non-joined rows are emitted by the RIGHT/FULL ALL path, which is the one that read the
-- null RowBitmaps. Keys 8 and 9 exist only on the right, so the bitmap is actually consulted.
INSERT INTO t04821_r SELECT number, concat('r', toString(number)) FROM numbers(8, 2);

SELECT '-- full with non-joined right rows: partial_merge vs hash';
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

SELECT '-- right with non-joined right rows: partial_merge vs hash';
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l RIGHT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge';
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l RIGHT JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

-- The spilled (on-disk) right side reaches the same finalization through mergeFlushedRightBlocks().
SELECT '-- full, spilled right side: partial_merge vs hash';
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'partial_merge', max_rows_in_join = 2, join_any_take_last_row = 0;
SELECT count(), countIf(rv != ''), countIf(lv = ''), sum(k)
FROM t04821_l AS l FULL JOIN (SELECT k, rv FROM t04821_r GROUP BY k, rv WITH TOTALS) AS r
USING (k)
SETTINGS join_algorithm = 'hash';

-- Totals themselves must still be produced: setTotals() keeps its IJoin part. `rsum` exists
-- only on the right, so joinTotals() has to take its totals value from the right side; a
-- column shared with the left would be sourced from left_totals and could not detect that.
SELECT '-- totals still work';
SELECT k, rsum FROM (SELECT k FROM t04821_l GROUP BY k WITH TOTALS) AS l
FULL JOIN (SELECT k, sum(k) AS rsum FROM t04821_r GROUP BY k WITH TOTALS) AS r USING (k)
ORDER BY k
SETTINGS join_algorithm = 'partial_merge';
SELECT k, rsum FROM (SELECT k FROM t04821_l GROUP BY k WITH TOTALS) AS l
FULL JOIN (SELECT k, sum(k) AS rsum FROM t04821_r GROUP BY k WITH TOTALS) AS r USING (k)
ORDER BY k
SETTINGS join_algorithm = 'hash';

DROP TABLE t04821_l;
DROP TABLE t04821_r;
