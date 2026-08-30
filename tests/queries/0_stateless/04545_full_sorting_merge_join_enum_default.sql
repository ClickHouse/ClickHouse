-- Tests that full_sorting_merge fills non-joined outer-join rows of Enum columns with the
-- type default (the first enum member), matching the other join algorithms, instead of a
-- raw 0 which is not a valid enum element (issue #111184: wrong results + UNKNOWN_ELEMENT_OF_ENUM).

DROP TABLE IF EXISTS tl;
DROP TABLE IF EXISTS tr;

CREATE TABLE tl (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE tr (s String, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY s;
INSERT INTO tl VALUES ('x'), ('y');
INSERT INTO tr VALUES ('x', 'b');

-- Reporter's exact repro: both must return 1 (the non-joined row's e is the type default 'a').
SELECT count() FROM tl AS l LEFT JOIN tr AS r ON l.s = r.s WHERE r.e = 'a' SETTINGS join_algorithm = 'hash';
SELECT count() FROM tl AS l LEFT JOIN tr AS r ON l.s = r.s WHERE r.e = 'a' SETTINGS join_algorithm = 'full_sorting_merge';

-- Selecting the padded column must not raise UNKNOWN_ELEMENT_OF_ENUM.
SELECT l.s, r.e FROM tl AS l LEFT JOIN tr AS r ON l.s = r.s ORDER BY l.s SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE tl;
DROP TABLE tr;

-- Cross-algorithm agreement for LEFT/RIGHT/FULL over Enum8/Enum16/Nullable(Enum), both
-- join_use_nulls modes. full_sorting_merge output must match hash exactly.

CREATE TABLE tl (s String, le Enum8('p' = 1, 'q' = 2)) ENGINE = MergeTree ORDER BY s;
CREATE TABLE tr (s String, e8 Enum8('a' = 1, 'b' = 2), e16 Enum16('x' = 100, 'y' = 200), ne Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY s;
INSERT INTO tl VALUES ('a', 'q'), ('b', 'q'), ('c', 'q');
INSERT INTO tr VALUES ('b', 'b', 'y', 'b'), ('d', 'a', 'x', NULL);

SELECT 'LEFT use_nulls=0';
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l LEFT JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.e8 SETTINGS join_algorithm = 'hash', join_use_nulls = 0;
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l LEFT JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.e8 SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 0;

SELECT 'RIGHT use_nulls=0';
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l RIGHT JOIN tr AS r ON l.s = r.s ORDER BY r.s, l.s SETTINGS join_algorithm = 'hash', join_use_nulls = 0;
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l RIGHT JOIN tr AS r ON l.s = r.s ORDER BY r.s, l.s SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 0;

SELECT 'FULL use_nulls=0';
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l FULL JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.s, r.e8 SETTINGS join_algorithm = 'hash', join_use_nulls = 0;
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l FULL JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.s, r.e8 SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 0;

SELECT 'FULL use_nulls=1';
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l FULL JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.s, r.e8 SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT l.s, l.le, r.e8, r.e16, r.ne FROM tl AS l FULL JOIN tr AS r ON l.s = r.s ORDER BY l.s, r.s, r.e8 SETTINGS join_algorithm = 'full_sorting_merge', join_use_nulls = 1;

DROP TABLE tl;
DROP TABLE tr;

-- USING join (key remap branch) with an Enum payload.
CREATE TABLE tl (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE tr (s String, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY s;
INSERT INTO tl VALUES ('x'), ('y');
INSERT INTO tr VALUES ('x', 'b');

SELECT 'USING FULL';
SELECT s, e FROM tl AS l FULL JOIN tr AS r USING (s) ORDER BY s SETTINGS join_algorithm = 'hash';
SELECT s, e FROM tl AS l FULL JOIN tr AS r USING (s) ORDER BY s SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE tl;
DROP TABLE tr;

-- ASOF LEFT JOIN with an Enum payload on the right (unmatched left row must default to 'a').
CREATE TABLE tl (k UInt32, t UInt32) ENGINE = MergeTree ORDER BY (k, t);
CREATE TABLE tr (k UInt32, t UInt32, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY (k, t);
INSERT INTO tl VALUES (1, 100), (2, 100);
INSERT INTO tr VALUES (1, 50, 'b');

SELECT 'ASOF LEFT';
SELECT l.k, r.e FROM tl AS l ASOF LEFT JOIN tr AS r ON l.k = r.k AND l.t >= r.t ORDER BY l.k SETTINGS join_algorithm = 'hash';
SELECT l.k, r.e FROM tl AS l ASOF LEFT JOIN tr AS r ON l.k = r.k AND l.t >= r.t ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge';

DROP TABLE tl;
DROP TABLE tr;
