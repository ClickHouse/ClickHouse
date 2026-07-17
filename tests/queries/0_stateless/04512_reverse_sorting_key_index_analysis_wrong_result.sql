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

-- Part without a final mark (non-adaptive granularity): the last mark range has no index value at its
-- end, so the storage-order right endpoint of a descending column is -inf there.
DROP TABLE IF EXISTS t_04512_nonadaptive;

CREATE TABLE t_04512_nonadaptive
(
    g String,
    r Enum8('poor' = 1, 'ok' = 2, 'great' = 3)
)
ENGINE = MergeTree
ORDER BY (g, r DESC)
SETTINGS allow_experimental_reverse_key = 1, index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_04512_nonadaptive VALUES ('manual', 'ok'), ('manual', 'poor'), ('novel', 'great'), ('novel', 'great');

SELECT count() FROM t_04512_nonadaptive WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nonadaptive WHERE g = 'novel' AND r = 'great' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nonadaptive WHERE r >= 'ok';
SELECT count() FROM t_04512_nonadaptive WHERE r < 'ok';

DROP TABLE t_04512_nonadaptive;

-- Nullable reverse key column: NULLs sort first in storage for a descending column. In the last mark
-- range of a part without a final mark, a NULL at the range begin must not collapse the column to the
-- point {NULL}: the range extends down to the smallest value.
DROP TABLE IF EXISTS t_04512_nullable;

CREATE TABLE t_04512_nullable (n Nullable(Int32))
ENGINE = MergeTree
ORDER BY n DESC
SETTINGS allow_experimental_reverse_key = 1, allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_04512_nullable VALUES (NULL), (NULL), (NULL), (5);

SELECT count() FROM t_04512_nullable WHERE n = 5 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable WHERE n = 5 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable WHERE n IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable WHERE n IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_04512_nullable;

-- Nullable reverse key, values then a trailing NULL across a mark boundary. NULL maps to +Inf and sorts
-- first for a descending column, so storage order is NULL, 4, 3 and no interior mark can have a value at
-- its begin and NULL at its end. A mark boundary on NULL must not build an empty value-space range and
-- prune the (4, 3) granule.
DROP TABLE IF EXISTS t_04512_nullable2;

CREATE TABLE t_04512_nullable2 (ts Nullable(Int32))
ENGINE = MergeTree
ORDER BY ts DESC
SETTINGS allow_experimental_reverse_key = 1, allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_04512_nullable2 VALUES (4), (3), (NULL);

SELECT count() FROM t_04512_nullable2 WHERE ts = 3 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable2 WHERE ts = 3 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable2 WHERE ts = 4 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable2 WHERE ts = 4 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable2 WHERE ts IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable2 WHERE ts IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable2 WHERE ts = 5 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable2 WHERE ts = 5 SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_04512_nullable2;

-- Composite key with a trailing descending Nullable column, mark boundary landing on NULL within a
-- leading-column group. Equality on the descending Nullable value must not prune its granule.
DROP TABLE IF EXISTS t_04512_nullable3;

CREATE TABLE t_04512_nullable3 (a Int32, n Nullable(Int32))
ENGINE = MergeTree
ORDER BY (a, n DESC)
SETTINGS allow_experimental_reverse_key = 1, allow_nullable_key = 1, index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Storage order within a=2 is NULL, 7, 4 (NULL sorts first for descending).
INSERT INTO t_04512_nullable3 VALUES (1, 5), (1, 3), (2, NULL), (2, 7), (2, 4), (3, 9);

SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n = 4 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n = 4 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n = 7 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n = 7 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_04512_nullable3 WHERE a = 2 AND n IS NULL SETTINGS use_lightweight_primary_key_index_analysis = 1;

DROP TABLE t_04512_nullable3;
