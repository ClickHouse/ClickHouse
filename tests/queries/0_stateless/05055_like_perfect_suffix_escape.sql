SET enable_analyzer = 1;
SET optimize_rewrite_like_perfect_affix = 1;

DROP TABLE IF EXISTS t_like_suffix_escape;

CREATE TABLE t_like_suffix_escape
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_like_suffix_escape VALUES
    ('q\\a-'),
    ('_a-'),
    ('fooa-'),
    ('fooa%');

-- Regression for #117257.
-- '_' is a wildcard and '\a' is an unknown escape which preserves '\'.
-- This must NOT be rewritten to endsWith.
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT s LIKE '%_\\a-' FROM t_like_suffix_escape;

SELECT s, s LIKE '%_\\a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

-- NOT LIKE must preserve the same semantics.
SELECT s, s NOT LIKE '%_\\a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

-- A normal perfect suffix must still be rewritten to endsWith.
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT s LIKE '%a-' FROM t_like_suffix_escape;

SELECT s, s LIKE '%a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

-- Escaped '%' is a literal percent sign.
-- '%a\%' is therefore a perfect suffix and must be rewritten to
-- endsWith(s, 'a%').
EXPLAIN SYNTAX run_query_tree_passes = 1
SELECT s LIKE '%a\\%' FROM t_like_suffix_escape;

SELECT s, s LIKE '%a\\%' AS matched
FROM t_like_suffix_escape
ORDER BY s;

DROP TABLE t_like_suffix_escape;
