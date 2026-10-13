SET output_format_pretty_color = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1(a String, b Date) ENGINE MergeTree order by a;
CREATE TABLE table2(c String, a String, d Date) ENGINE MergeTree order by c;

INSERT INTO table1 VALUES ('a', '2018-01-01') ('b', '2018-01-01') ('c', '2018-01-01');
INSERT INTO table2 VALUES ('D', 'd', '2018-01-01') ('B', 'b', '2018-01-01') ('C', 'c', '2018-01-01');

-- The test only cares about the column names in the result header, but `PrettyCompact` starts a new
-- table for every chunk it receives, and consecutive chunks are glued together only if they arrive
-- within `output_format_pretty_squash_consecutive_ms`. The `ALL LEFT JOIN` below emits its single
-- non-joined row in a chunk of its own, so on a loaded machine the result is printed as two tables.
-- `MonoBlock` squashes everything into one table regardless of chunking and timing.
SELECT * FROM table1 t1 FORMAT PrettyCompactMonoBlock;
SELECT *, c as a, d as b FROM table2 FORMAT PrettyCompactMonoBlock;
SELECT * FROM table1 t1 ALL LEFT JOIN (SELECT *, c, d as b FROM table2) t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompactMonoBlock;
SELECT * FROM table1 t1 ALL INNER JOIN (SELECT *, c, d as b FROM table2) t2 USING (a, b) ORDER BY d, t1.a FORMAT PrettyCompactMonoBlock;

DROP TABLE table1;
DROP TABLE table2;
