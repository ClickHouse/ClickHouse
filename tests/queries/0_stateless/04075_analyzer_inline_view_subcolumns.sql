-- Test that subcolumn reads work correctly with VIEW subquery inlining.

SET enable_analyzer = 1;
SET analyzer_inline_views = 1;

DROP TABLE IF EXISTS t_subcol_src;

CREATE TABLE t_subcol_src
(
    arr Array(UInt32),
    tup Tuple(s String, u UInt32),
    n   Nullable(UInt32),
    m   Map(String, UInt32)
)
ENGINE = MergeTree ORDER BY ();

INSERT INTO t_subcol_src VALUES ([1, 2, 3], ('hello', 10), 42,    map('a', 1, 'b', 2));
INSERT INTO t_subcol_src VALUES ([],        ('world', 20), NULL,  map('c', 3));
INSERT INTO t_subcol_src VALUES ([4, 5],    ('foo',   30), 100,   map('d', 4, 'e', 5, 'f', 6));

-- View with matching types (no CAST wrapper).
DROP VIEW IF EXISTS v_subcol;
CREATE VIEW v_subcol AS SELECT arr, tup, n, m FROM t_subcol_src;

SELECT '-- array.size0 from view (inlined)';
SELECT arr.size0 FROM v_subcol ORDER BY arr.size0;

SELECT '-- tuple elements from view (inlined)';
SELECT tup.s, tup.u FROM v_subcol ORDER BY tup.u;

SELECT '-- nullable.null from view (inlined)';
SELECT n.null FROM v_subcol ORDER BY n.null;

SELECT '-- map subcolumns from view (inlined)';
SELECT arraySort(m.keys) FROM v_subcol ORDER BY length(m.keys);
SELECT arraySort(m.values) FROM v_subcol ORDER BY length(m.values);

-- Same queries without inlining to verify correctness.
SET analyzer_inline_views = 0;

SELECT '-- array.size0 from view (not inlined)';
SELECT arr.size0 FROM v_subcol ORDER BY arr.size0;

SELECT '-- tuple elements from view (not inlined)';
SELECT tup.s, tup.u FROM v_subcol ORDER BY tup.u;

SELECT '-- nullable.null from view (not inlined)';
SELECT n.null FROM v_subcol ORDER BY n.null;

SELECT '-- map subcolumns from view (not inlined)';
SELECT arraySort(m.keys) FROM v_subcol ORDER BY length(m.keys);
SELECT arraySort(m.values) FROM v_subcol ORDER BY length(m.values);

-- View with type conversion: source has Array(UInt16) but view declares Array(UInt64).
SET analyzer_inline_views = 1;

DROP TABLE IF EXISTS t_subcol_src2;
CREATE TABLE t_subcol_src2
(
    arr Array(UInt16),
    n   Nullable(UInt16),
    tup Tuple(s String, u UInt16)
)
ENGINE = MergeTree ORDER BY ();

INSERT INTO t_subcol_src2 VALUES ([10, 20], 5,    ('aaa', 1));
INSERT INTO t_subcol_src2 VALUES ([],       NULL, ('bbb', 2));
INSERT INTO t_subcol_src2 VALUES ([30],     100,  ('ccc', 3));

DROP VIEW IF EXISTS v_subcol_wider;
CREATE VIEW v_subcol_wider (arr Array(UInt64), n Nullable(UInt64), tup Tuple(s String, u UInt64)) AS SELECT arr, n, tup FROM t_subcol_src2;

SELECT '-- subcolumns from view with type conversion (inlined)';
SELECT arr.size0 FROM v_subcol_wider ORDER BY arr.size0;
SELECT tup.s, tup.u FROM v_subcol_wider ORDER BY tup.u;
SELECT n.null FROM v_subcol_wider ORDER BY n.null;

SET analyzer_inline_views = 0;

SELECT '-- subcolumns from view with type conversion (not inlined)';
SELECT arr.size0 FROM v_subcol_wider ORDER BY arr.size0;
SELECT tup.s, tup.u FROM v_subcol_wider ORDER BY tup.u;
SELECT n.null FROM v_subcol_wider ORDER BY n.null;

DROP VIEW v_subcol_wider;
DROP TABLE t_subcol_src2;
DROP VIEW v_subcol;
DROP TABLE t_subcol_src;
