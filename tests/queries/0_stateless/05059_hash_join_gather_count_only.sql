-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- A join that selects `count()` puts no right column in the output, so the emit has nothing to
-- append. The join still records row refs, because `EXPLAIN ANALYZE matches = 1` counts the matches
-- from them. All three emit builders have to tolerate that empty output, and which one runs depends
-- on settings the test runner randomizes, so pin them and walk all three.
--
-- Both sides stay under a thousand rows. Above that a count prints as `1.00 thousand`, which the
-- regex below reads as 1.

SET enable_analyzer = 1;
-- Swapping the sides makes the join add the other side's columns, and the output is no longer empty.
SET query_plan_join_swap_table = 0;
-- A runtime filter would drop the non-matching left rows before the probe.
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS gco_left;
DROP TABLE IF EXISTS gco_right;
CREATE TABLE gco_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE gco_right (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Ten right rows per key, so the first arm's threshold of zero selects the row-list builder.
INSERT INTO gco_left SELECT number, number FROM numbers(90);
INSERT INTO gco_right SELECT number % 90, number FROM numbers(900);

SELECT 'ground truth', count(), uniqExact(k) FROM gco_left ALL INNER JOIN gco_right USING (k);

SELECT 'by row lists',
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%'),
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%')
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM gco_left ALL INNER JOIN gco_right USING (k)
      SETTINGS join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0);

SELECT 'by blocks',
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%'),
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%')
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM gco_left ALL INNER JOIN gco_right USING (k)
      SETTINGS join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0);

SELECT 'by limit and offset',
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Left: rows%'),
       maxIf(extract(explain, 'matched (not collected|[0-9]+)'), explain LIKE '%Right: rows%')
FROM (EXPLAIN ANALYZE matches = 1 SELECT count() FROM gco_left ALL INNER JOIN gco_right USING (k)
      SETTINGS join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 1);

DROP TABLE gco_left;
DROP TABLE gco_right;
