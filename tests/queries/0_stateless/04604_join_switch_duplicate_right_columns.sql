-- Regression test for a crash when `join_algorithm = 'auto'` switches an in-memory hash join to
-- a merge join (`JoinSwitcher`) and the right-hand side has several columns with the same name.
--
-- Switching restructures the stored right-side blocks: for every column of the right sample block
-- it picks the matching stored column by name. When two right columns share a name they resolve to
-- the same stored position, so that position is read more than once. The stored column must be
-- copied, not moved, otherwise the second read installs a null column and the join aborts in
-- `materializeBlock`.
--
-- `max_bytes_before_external_join = 0` / `max_bytes_ratio_before_external_join = 0` keep the `auto`
-- algorithm on the `JoinSwitcher` path (rather than the spilling / grace path), and
-- `max_rows_in_join = 1` with two right rows forces the switch to the merge join.

SELECT * FROM (SELECT 1 AS id, '' AS test) AS a
LEFT JOIN (SELECT test, 1 AS id, NULL AS test UNION ALL SELECT test, 2 AS id, NULL AS test) AS b ON b.id = a.id
SETTINGS join_algorithm = 'auto', max_rows_in_join = 1,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_analyzer = 0
;

SELECT * FROM (SELECT 1 AS id, '' AS test) AS a
LEFT JOIN (SELECT test, 1 AS id, NULL AS test UNION ALL SELECT test, 2 AS id, NULL AS test) AS b ON b.id = a.id
SETTINGS join_algorithm = 'auto', max_rows_in_join = 1,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_analyzer = 1
;
