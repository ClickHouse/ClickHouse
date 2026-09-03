CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY ((c0 AS x)); -- { clientError SYNTAX_ERROR }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 AS x); -- { clientError SYNTAX_ERROR }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 AS x) DESC; -- { clientError SYNTAX_ERROR }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS x; -- { clientError SYNTAX_ERROR }
CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS x DESC; -- { clientError SYNTAX_ERROR }
DELETE FROM t0 WHERE (true AS a0); -- { clientError SYNTAX_ERROR }
DELETE FROM t0 WHERE true AS a0; -- { clientError SYNTAX_ERROR }
