-- Tags: distributed

-- `merge` reads each of its tables with a copy of the query in which the `JOIN` is removed. A child that
-- delegates further, such as a `Distributed` table, analyzes that copy again, so it must not keep the
-- `LIMIT AFTER`/`UNTIL` boundaries: they may refer to columns of the removed joined table, and they select
-- rows relative to an `ORDER BY` that the copy no longer has. The rows must be the same as when the
-- underlying table is read directly.
DROP TABLE IF EXISTS t_range_merge_local;
DROP TABLE IF EXISTS t_range_merge_dist;
CREATE TABLE t_range_merge_local (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_range_merge_local SELECT number FROM numbers(6);
CREATE TABLE t_range_merge_dist AS t_range_merge_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_range_merge_local);

SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT 2 AFTER r.number >= 2 SETTINGS enable_analyzer = 1;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT UNTIL r.number >= 2 SETTINGS enable_analyzer = 1;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT 1 AFTER r.number IN (1, 3) ALL SETTINGS enable_analyzer = 1;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n DESC LIMIT 2 AFTER l.n <= 3 SETTINGS enable_analyzer = 1;

SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT 2 AFTER r.number >= 2 SETTINGS enable_analyzer = 0;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT UNTIL r.number >= 2 SETTINGS enable_analyzer = 0;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n LIMIT 1 AFTER r.number IN (1, 3) ALL SETTINGS enable_analyzer = 0;
SELECT l.n, r.number FROM merge(currentDatabase(), '^t_range_merge_dist$') AS l INNER JOIN numbers(6) AS r ON l.n = r.number ORDER BY l.n DESC LIMIT 2 AFTER l.n <= 3 SETTINGS enable_analyzer = 0;

DROP TABLE t_range_merge_dist;
DROP TABLE t_range_merge_local;
