SET optimize_rewrite_like_perfect_affix = 1;

CREATE TABLE t_like_suffix_escape
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_like_suffix_escape VALUES
    ('q\\a-'),
    ('_a-');

-- Regression for #117257.
-- This pattern contains a real '_' wildcard followed by an unknown '\a'
-- escape. Reversing the pattern would incorrectly turn it into '\_'.
SELECT s, s LIKE '%_\\a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

-- Make sure NOT LIKE has the same correct semantics.
SELECT s, s NOT LIKE '%_\\a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

-- A normal perfect suffix must still be optimized correctly.
SELECT s, s LIKE '%a-' AS matched
FROM t_like_suffix_escape
ORDER BY s;

DROP TABLE t_like_suffix_escape;
