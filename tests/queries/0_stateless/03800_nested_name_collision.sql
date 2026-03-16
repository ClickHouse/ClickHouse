-- Dotted Array columns (e.g. `n.a Array(String)`) should be treated as parts
-- of a Nested structure sharing offsets, even when a non-Array column with
-- the prefix name exists (e.g. `n String`).
-- See https://github.com/ClickHouse/ClickHouse/issues/93777

-- Case 1: single dotted Array + scalar prefix (the original reported issue).
DROP TABLE IF EXISTS nested_collision_string;
CREATE TABLE nested_collision_string (n String, `n.a` Array(String))
ENGINE = MergeTree ORDER BY n;
INSERT INTO nested_collision_string VALUES ('Hello', ['World', 'Test']);
SELECT * FROM nested_collision_string;
SELECT n FROM nested_collision_string;
SELECT `n.a` FROM nested_collision_string;
SELECT `n.a`.size0 FROM nested_collision_string;
DROP TABLE nested_collision_string;

-- Case 2: multiple dotted Arrays + scalar prefix.
-- The arrays share offsets (`n.size0`) as parts of a Nested structure.
DROP TABLE IF EXISTS nested_collision_multi;
CREATE TABLE nested_collision_multi (n UInt32, `n.a` Array(String), `n.b` Array(UInt32))
ENGINE = MergeTree ORDER BY n;
INSERT INTO nested_collision_multi VALUES (1, ['x', 'y'], [10, 20]);
INSERT INTO nested_collision_multi VALUES (2, ['z'], [30]);
SELECT * FROM nested_collision_multi ORDER BY n;
SELECT n FROM nested_collision_multi ORDER BY n;
SELECT `n.a` FROM nested_collision_multi ORDER BY `n.a`;
SELECT `n.b` FROM nested_collision_multi ORDER BY `n.b`;
SELECT `n.a`.size0, `n.b`.size0 FROM nested_collision_multi ORDER BY `n.a`.size0;
-- Verify the arrays share offsets (sizes must be equal per row).
SELECT `n.a`.size0 = `n.b`.size0 FROM nested_collision_multi;
DROP TABLE nested_collision_multi;

-- Case 3: normal flat-nested must still work (no prefix column exists,
-- so `n.a` and `n.b` are grouped into Nested with shared offsets).
DROP TABLE IF EXISTS nested_normal;
CREATE TABLE nested_normal (`n.a` Array(String), `n.b` Array(UInt32))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nested_normal VALUES (['x', 'y'], [1, 2]);
SELECT * FROM nested_normal;
SELECT `n.a` FROM nested_normal;
SELECT `n.b` FROM nested_normal;
SELECT `n.a`.size0, `n.b`.size0 FROM nested_normal;
DROP TABLE nested_normal;
