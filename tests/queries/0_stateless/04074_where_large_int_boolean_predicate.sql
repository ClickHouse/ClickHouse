-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101269
-- A large integer constant used as a boolean predicate in WHERE with AND
-- was incorrectly truncated to UInt8 during MergeTree virtual column filter
-- analysis, causing all parts to be pruned and returning 0 rows.

DROP TABLE IF EXISTS t_large_int_bool;
CREATE TABLE t_large_int_bool (b Int8) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_large_int_bool VALUES (1), (0);

-- All of these should return 2 rows (both values satisfy the condition).
-- The integer literal is truthy (non-zero) and should not be truncated to UInt8.
SELECT count() FROM t_large_int_bool WHERE (256 > b) AND 256;
SELECT count() FROM t_large_int_bool WHERE (512 > b) AND 512;
SELECT count() FROM t_large_int_bool WHERE (65536 > b) AND 65536;
SELECT count() FROM t_large_int_bool WHERE (2147483648 > b) AND 2147483648;
SELECT count() FROM t_large_int_bool WHERE 2147483648 AND (2147483648 > b);

-- countIf variant (always worked — the bug was MergeTree-specific)
SELECT countIf((2147483648 > b) AND 2147483648) FROM t_large_int_bool;

-- Verify falsy constants still correctly prune (AND 0 should return 0 rows)
SELECT count() FROM t_large_int_bool WHERE (256 > b) AND 0;

-- Verify negative numbers work as truthy predicates
SELECT count() FROM t_large_int_bool WHERE (256 > b) AND -1;

-- Verify float values work as truthy predicates
SELECT count() FROM t_large_int_bool WHERE (256 > b) AND 0.5;

DROP TABLE t_large_int_bool;
