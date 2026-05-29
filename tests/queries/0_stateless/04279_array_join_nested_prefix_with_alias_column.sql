-- Regression test: `ARRAY JOIN <name>` over a nested prefix should expand to the
-- per-field Array columns even when an ALIAS column with the same name exists.
-- The new analyzer used to pick the ALIAS column and fail with `TYPE_MISMATCH`
-- when the alias resolved to a non-Array expression (e.g. a Tuple).

DROP TABLE IF EXISTS t_array_join_alias_nested SYNC;

CREATE TABLE t_array_join_alias_nested
(
    id String,
    `loc.x` Array(String),
    `loc.y` Array(String),
    -- ALIAS column with the same name as the Nested prefix.
    loc Tuple(x String, y String) ALIAS tuple(`loc.x`[1], `loc.y`[1])
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_array_join_alias_nested (id, `loc.x`, `loc.y`)
VALUES ('a', ['x1', 'x2'], ['y1', 'y2']);

SELECT loc.x FROM t_array_join_alias_nested ARRAY JOIN loc
SETTINGS enable_analyzer = 0;

SELECT loc.x FROM t_array_join_alias_nested ARRAY JOIN loc
SETTINGS enable_analyzer = 1;

SELECT loc.x, loc.y FROM t_array_join_alias_nested ARRAY JOIN loc
SETTINGS enable_analyzer = 1;

-- Identifier qualified by table alias must be stripped before retrying as Nested prefix.
SELECT s.loc.x FROM t_array_join_alias_nested AS s ARRAY JOIN s.loc
SETTINGS enable_analyzer = 1;

-- Identifier qualified by table name.
SELECT t_array_join_alias_nested.loc.x
FROM t_array_join_alias_nested
ARRAY JOIN t_array_join_alias_nested.loc
SETTINGS enable_analyzer = 1;

DROP TABLE t_array_join_alias_nested;
