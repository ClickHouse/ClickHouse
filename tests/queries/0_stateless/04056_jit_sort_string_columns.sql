-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

DROP TABLE IF EXISTS t_jit_sort_str;

CREATE TABLE t_jit_sort_str
(
    s String,
    fs FixedString(4),
    ns Nullable(String),
    nfs Nullable(FixedString(4)),
    x UInt32
) ENGINE = Memory;

INSERT INTO t_jit_sort_str VALUES ('banana', 'bana', 'banana', 'bana', 3);
INSERT INTO t_jit_sort_str VALUES ('apple', 'appl', 'apple', 'appl', 1);
INSERT INTO t_jit_sort_str VALUES ('cherry', 'cher', 'cherry', 'cher', 2);
INSERT INTO t_jit_sort_str VALUES ('apple', 'appl', NULL, NULL, 4);
INSERT INTO t_jit_sort_str VALUES ('banana', 'bana', NULL, NULL, 5);
INSERT INTO t_jit_sort_str VALUES ('', 'aaaa', '', 'aaaa', 6);
INSERT INTO t_jit_sort_str VALUES ('cherry', 'cher', 'cherry', 'cher', 7);
INSERT INTO t_jit_sort_str VALUES ('apple', 'bana', 'apple', 'bana', 8);

SELECT '--- String ASC ---';
SELECT s, x FROM t_jit_sort_str ORDER BY s, x;

SELECT '--- String DESC ---';
SELECT s, x FROM t_jit_sort_str ORDER BY s DESC, x;

SELECT '--- FixedString ASC ---';
SELECT fs, x FROM t_jit_sort_str ORDER BY fs, x;

SELECT '--- FixedString DESC ---';
SELECT fs, x FROM t_jit_sort_str ORDER BY fs DESC, x;

SELECT '--- Nullable(String) ASC NULLS FIRST ---';
SELECT ns, x FROM t_jit_sort_str ORDER BY ns ASC NULLS FIRST, x;

SELECT '--- Nullable(String) ASC NULLS LAST ---';
SELECT ns, x FROM t_jit_sort_str ORDER BY ns ASC NULLS LAST, x;

SELECT '--- Nullable(FixedString) ASC NULLS FIRST ---';
SELECT nfs, x FROM t_jit_sort_str ORDER BY nfs ASC NULLS FIRST, x;

SELECT '--- Nullable(FixedString) ASC NULLS LAST ---';
SELECT nfs, x FROM t_jit_sort_str ORDER BY nfs ASC NULLS LAST, x;

SELECT '--- Mixed: String + UInt32 ---';
SELECT s, x FROM t_jit_sort_str ORDER BY s, x DESC;

SELECT '--- Mixed: UInt32 + FixedString ---';
SELECT x, fs FROM t_jit_sort_str ORDER BY x, fs;

SELECT '--- Mixed: Nullable(String) + FixedString ---';
SELECT ns, fs, x FROM t_jit_sort_str ORDER BY ns ASC NULLS LAST, fs, x;

SELECT '--- Empty string edge case ---';
SELECT s, x FROM t_jit_sort_str WHERE s = '' OR s = 'apple' ORDER BY s, x;

DROP TABLE t_jit_sort_str;
