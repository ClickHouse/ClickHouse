-- Reverse (descending) sorting key: primary index analysis must not prune a granule that
-- spans a change in a leading key column when a following column is descending.
-- Regression for wrong result under filter push-down / equality on a reverse key (issue #110275).

DROP TABLE IF EXISTS t_04512;

CREATE TABLE t_04512
(
    g String,
    r Enum8('poor' = 1, 'ok' = 2, 'great' = 3)
)
ENGINE = MergeTree
ORDER BY (g, r DESC)
SETTINGS allow_experimental_reverse_key = 1, index_granularity = 8192;

-- One granule holding two 'g' values; within 'novel' the descending 'r' is 'great'(3).
INSERT INTO t_04512 VALUES ('manual', 'ok'), ('manual', 'poor'), ('novel', 'great'), ('novel', 'great');

-- Equality on both key columns: must return the 2 'novel'/'great' rows, not prune them.
SELECT count() FROM t_04512 WHERE g = 'novel' AND r = 'great';
SELECT count() FROM t_04512 WHERE g = 'manual' AND r = 'ok';
SELECT count() FROM t_04512 WHERE g = 'manual' AND r = 'poor';

-- Range predicates on the descending column across the prefix boundary.
SELECT count() FROM t_04512 WHERE r >= 'ok';
SELECT count() FROM t_04512 WHERE r < 'ok';

-- Same shape must be stable regardless of the lightweight (sparse) index analysis.
SELECT count() FROM t_04512 WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512 WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_04512;

-- The original issue shape: filter push-down of a NOT IN onto a reverse-keyed table via ANY JOIN + CTE.
-- The result must match with filter push-down enabled and disabled.
DROP TABLE IF EXISTS t_04512_authors;
DROP TABLE IF EXISTS t_04512_books;
DROP TABLE IF EXISTS t_04512_publishers;
DROP TABLE IF EXISTS t_04512_blocked;

CREATE TABLE t_04512_authors (name String) ENGINE = MergeTree ORDER BY name;

CREATE TABLE t_04512_books
(
    publisher_id UInt32,
    genre LowCardinality(String),
    author_name String,
    review Enum8('poor' = 1, 'ok' = 2, 'great' = 3),
    copies_sold UInt32
)
ENGINE = MergeTree
ORDER BY (genre, review DESC, copies_sold DESC, cityHash64(author_name))
SETTINGS allow_experimental_reverse_key = 1;

CREATE TABLE t_04512_publishers (id UInt32, active Bool) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_04512_blocked (list_name String, author_name String) ENGINE = MergeTree ORDER BY list_name;

INSERT INTO t_04512_authors SELECT 'author' || leftPad(toString(number), 2, '0') FROM numbers(1, 25);
INSERT INTO t_04512_books SELECT 2, 'manual', 'other' || toString(number), 'poor', number FROM numbers(7);
INSERT INTO t_04512_books SELECT 1, 'novel', 'author' || leftPad(toString(number), 2, '0'), 'great', 300 - number FROM numbers(1, 25);
INSERT INTO t_04512_publishers VALUES (1, true);
INSERT INTO t_04512_blocked VALUES ('empty-list', 'not-a-real-author');

WITH matching_books AS
(
    SELECT b.author_name
    FROM t_04512_books AS b
    ANY LEFT JOIN t_04512_publishers AS p ON p.id = b.publisher_id
    WHERE b.genre = 'novel' AND b.review IN ('great') AND p.active = true
)
SELECT count()
FROM t_04512_authors AS a
ANY INNER JOIN matching_books AS b ON a.name = b.author_name
WHERE a.name NOT IN (SELECT author_name FROM t_04512_blocked WHERE list_name = 'empty-list')
SETTINGS query_plan_filter_push_down = 1;

WITH matching_books AS
(
    SELECT b.author_name
    FROM t_04512_books AS b
    ANY LEFT JOIN t_04512_publishers AS p ON p.id = b.publisher_id
    WHERE b.genre = 'novel' AND b.review IN ('great') AND p.active = true
)
SELECT count()
FROM t_04512_authors AS a
ANY INNER JOIN matching_books AS b ON a.name = b.author_name
WHERE a.name NOT IN (SELECT author_name FROM t_04512_blocked WHERE list_name = 'empty-list')
SETTINGS query_plan_filter_push_down = 0;

DROP TABLE t_04512_authors;
DROP TABLE t_04512_books;
DROP TABLE t_04512_publishers;
DROP TABLE t_04512_blocked;
