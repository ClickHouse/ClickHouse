-- Inside a lambda whose argument shadows the renamed column, `RENAME COLUMN` must leave the plain
-- identifiers alone, but still rewrite the column names kept by the `EXCEPT`
-- transformer of a matcher over table columns (`*`, `COLUMNS('regexp')`). The identifiers listed in
-- `COLUMNS(a, b)` are resolved in the lambda scope, like in a `SELECT`, so they and their transformers
-- keep referring to the lambda argument.

SET enable_named_columns_in_function_tuple = 0;

DROP TABLE IF EXISTS t_rename_shadowed_matcher;

CREATE TABLE t_rename_shadowed_matcher
(
    a UInt64,
    b UInt64,
    c UInt64,
    d_regexp_except String DEFAULT toJSONString(arrayMap(a -> tuple(a, COLUMNS('^(a|b|c|renamed)$') EXCEPT a), [100])),
    d_list String DEFAULT toJSONString(arrayMap(a -> tuple(COLUMNS(a, b)), [100])),
    d_list_except String DEFAULT toJSONString(arrayMap(a -> tuple(COLUMNS(b, c, a) EXCEPT a), [100]))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_rename_shadowed_matcher (a, b, c) VALUES (1, 2, 3);

ALTER TABLE t_rename_shadowed_matcher RENAME COLUMN a TO renamed;

SELECT name, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_rename_shadowed_matcher' AND name LIKE 'd_%'
ORDER BY name;

-- The values for the second row must have the same shape as for the first one.
INSERT INTO t_rename_shadowed_matcher (renamed, b, c) VALUES (10, 20, 30);
SELECT renamed, b, c, d_regexp_except, d_list, d_list_except FROM t_rename_shadowed_matcher ORDER BY renamed;

DROP TABLE t_rename_shadowed_matcher;
