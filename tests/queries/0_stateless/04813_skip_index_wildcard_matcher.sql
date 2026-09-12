-- Tags: no-parallel
-- - no-parallel -- SQL UDFs are global server objects; the flaky check runs the same test concurrently and the CREATE FUNCTION statements would collide.

-- Regression test: wildcards and column matchers must be rejected in skip index
-- expressions. The stored `definition_ast` keeps the matcher text and the index is
-- rebuilt from it on column-layout changes (`recalculateWithNewColumns`), so
-- `ALTER TABLE ... ADD COLUMN` would silently resolve the matcher to a different
-- column set while existing parts keep index files built with the previous schema.

DROP TABLE IF EXISTS t_skip_index_matcher;

-- A matcher that resolves to exactly one column: the resolved-output count looks
-- plausible, but the definition stays schema-dependent.
CREATE TABLE t_skip_index_matcher
(
    a UInt64,
    b UInt64,
    INDEX idx COLUMNS('^a$') TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- Wildcard.
CREATE TABLE t_skip_index_matcher
(
    a UInt64,
    b UInt64,
    INDEX idx (*) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- A matcher nested inside a function call.
CREATE TABLE t_skip_index_matcher
(
    a UInt64,
    b UInt64,
    INDEX idx tuple(COLUMNS('^a$')) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a; -- { serverError BAD_ARGUMENTS }

-- `ALTER TABLE ... ADD INDEX` takes the same path.
CREATE TABLE t_skip_index_matcher
(
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE t_skip_index_matcher ADD INDEX idx COLUMNS('^a$') TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

-- A matcher hidden in a SQL UDF is inlined by the DDL interpreter before the
-- table metadata is built, so it is rejected the same way.
CREATE FUNCTION f_04813_matcher AS () -> COLUMNS('^a$');
ALTER TABLE t_skip_index_matcher ADD INDEX idx f_04813_matcher() TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
DROP FUNCTION f_04813_matcher;

-- A regular skip index still works and survives a column-layout change.
CREATE TABLE t_skip_index_ok
(
    a UInt64,
    b UInt64,
    INDEX idx a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO t_skip_index_ok SELECT number, number FROM numbers(10);
ALTER TABLE t_skip_index_ok ADD COLUMN c UInt64 DEFAULT 0;
SELECT count() FROM t_skip_index_ok WHERE a = 5 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_skip_index_matcher;
DROP TABLE t_skip_index_ok;
