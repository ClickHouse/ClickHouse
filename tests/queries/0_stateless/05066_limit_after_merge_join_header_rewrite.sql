-- With the legacy analyzer, `merge` derives the header of its query from a copy of the query without the
-- `JOIN`. That copy must not keep `LIMIT AFTER`/`UNTIL` boundaries that refer to the joined table.
SET max_block_size = 1;

DROP TABLE IF EXISTS t_merge_range_left;
CREATE TABLE t_merge_range_left (x UInt64) ENGINE = Memory;
INSERT INTO t_merge_range_left SELECT number FROM numbers(5);

SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT AFTER r.number >= 2 SETTINGS enable_analyzer = 0;
SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT UNTIL r.number >= 2 SETTINGS enable_analyzer = 0;
SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT 1 AFTER r.number IN (1, 3) ALL SETTINGS enable_analyzer = 0;

SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT AFTER r.number >= 2 SETTINGS enable_analyzer = 1;
SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT UNTIL r.number >= 2 SETTINGS enable_analyzer = 1;
SELECT l.x FROM merge(currentDatabase(), '^t_merge_range_left$') AS l INNER JOIN numbers(5) AS r ON l.x = r.number ORDER BY l.x LIMIT 1 AFTER r.number IN (1, 3) ALL SETTINGS enable_analyzer = 1;

DROP TABLE t_merge_range_left;
