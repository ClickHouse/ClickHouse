-- Checks that `EXPLAIN ANALYZE matches = 1` reports the same matched-row counts for the same data
-- whatever the join kind and the join algorithm, and reports `not collected` for the kinds where an
-- exact count cannot be derived.
--
-- Both sides have 1000 rows, but only keys 0..99 are shared, so 439 left rows and 521 right rows
-- find a partner. The two counts differ from each other and from the table sizes, so a metric that
-- silently falls back to the row count, to zero, or to the other side cannot pass. The ASOF
-- condition holds for every shared key, so ASOF matches the same 439 left rows.
--
-- The join queries select `count()` on purpose: with no right column in the output the probe has
-- nothing to materialize, which is the path where the counts are hardest to obtain.

SET enable_analyzer = 1;
-- Swapping the sides moves `columns_added_by_join` and flips which side owns the output filter.
SET query_plan_join_swap_table = 0;
-- A runtime filter would drop non-matching left rows before the probe, changing `Left: rows`.
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS tl;
DROP TABLE IF EXISTS tr;
CREATE TABLE tl (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tr (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO tl SELECT number % 100, number FROM numbers(439);
INSERT INTO tl SELECT 1000 + number, number FROM numbers(561);
INSERT INTO tr SELECT number % 100, number FROM numbers(521);
INSERT INTO tr SELECT 2000 + number, number FROM numbers(479);

SELECT 'ground truth: rows, matched';
SELECT count(), countIf(k IN (SELECT k FROM tr)) FROM tl;
SELECT count(), countIf(k IN (SELECT k FROM tl)) FROM tr;

SELECT 'hash';
SELECT 'ALL INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL INNER JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ALL RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL FULL JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'SEMI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl SEMI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'SEMI RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl SEMI RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ANTI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ANTI RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ANY INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANY INNER JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ANY LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANY LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ANY RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANY RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ASOF' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ASOF JOIN tr ON tl.k = tr.k AND tl.v >= tr.v SETTINGS join_algorithm = 'hash');
SELECT 'ASOF LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ASOF LEFT JOIN tr ON tl.k = tr.k AND tl.v >= tr.v SETTINGS join_algorithm = 'hash');

SELECT 'parallel_hash';
SELECT 'ALL INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL INNER JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ALL RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL FULL JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'SEMI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl SEMI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ANTI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ANTI RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);
SELECT 'ANY RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANY RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 8);

SELECT 'grace_hash';
SELECT 'ALL INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL INNER JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ALL RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL FULL JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'SEMI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl SEMI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ANTI LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ANTI RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANTI RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);
SELECT 'ANY RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ANY RIGHT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'grace_hash', max_bytes_before_external_join = 1);

-- Splitting the output into single-row chunks exercises the per-chunk ref bookkeeping; the reported
-- totals must not change.
SELECT 'single-row chunks';
SELECT 'ALL INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL INNER JOIN tr ON tl.k = tr.k SETTINGS joined_block_split_single_row = 1, max_joined_block_size_rows = 1);
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL LEFT JOIN tr ON tl.k = tr.k SETTINGS joined_block_split_single_row = 1, max_joined_block_size_rows = 1);
SELECT 'ALL RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL RIGHT JOIN tr ON tl.k = tr.k SETTINGS joined_block_split_single_row = 1, max_joined_block_size_rows = 1);
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL FULL JOIN tr ON tl.k = tr.k SETTINGS joined_block_split_single_row = 1, max_joined_block_size_rows = 1);

-- A residual `ON` condition routes the probe through a separate loop. Only shared keys can satisfy
-- `tl.v >= tr.v`, and every shared-key row does, so the counts stay the same.
SELECT 'residual condition';
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL LEFT JOIN tr ON tl.k = tr.k AND tl.v >= tr.v SETTINGS join_algorithm = 'hash');
SELECT 'ALL RIGHT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL RIGHT JOIN tr ON tl.k = tr.k AND tl.v >= tr.v SETTINGS join_algorithm = 'hash');
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM tl ALL FULL JOIN tr ON tl.k = tr.k AND tl.v >= tr.v SETTINGS join_algorithm = 'hash');

-- With a right column in the output the refs are recorded for materialization anyway, so the counts
-- must not depend on the projection either.
SELECT 'right column in the output';
SELECT 'ALL INNER' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT sum(tr.v) FROM tl ALL INNER JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ALL LEFT' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT sum(tr.v) FROM tl ALL LEFT JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');
SELECT 'ALL FULL' AS kind,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%') AS matched_left,
    maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%') AS matched_right
FROM (EXPLAIN ANALYZE matches = 1 SELECT sum(tr.v) FROM tl ALL FULL JOIN tr ON tl.k = tr.k SETTINGS join_algorithm = 'hash');

DROP TABLE tl;
DROP TABLE tr;
