-- Matcher expansion in column DEFAULT/MATERIALIZED expressions must be validated against the same
-- (flattened) schema that is used when the stored default is evaluated at insert time. The matcher is
-- stored unexpanded and re-expanded on every evaluation, so if validation sees the un-flattened schema
-- it can accept a default that expands differently -- or to nothing -- against the flattened schema at
-- insert time. See https://github.com/ClickHouse/ClickHouse/pull/105045

SET flatten_nested = 1;

DROP TABLE IF EXISTS t_matcher_flatten;
DROP TABLE IF EXISTS t_matcher_flatten_mat;

-- The matcher matches the flattened subcolumn `n.x`, exactly as it does when the default is evaluated
-- at insert time, so validation and execution agree and the default is computed correctly.
-- Note: against the un-flattened schema the column would be `n`, so `COLUMNS('x')` would match nothing
-- and validation would wrongly reject this table.
CREATE TABLE t_matcher_flatten
(
    n Nested(x UInt8),
    cnt UInt64 DEFAULT length(COLUMNS('x'))
)
ENGINE = Memory;

INSERT INTO t_matcher_flatten (`n.x`) VALUES ([10, 20, 30]);
SELECT cnt FROM t_matcher_flatten;

-- Same for a MATERIALIZED expression.
CREATE TABLE t_matcher_flatten_mat
(
    n Nested(x UInt8),
    cnt UInt64 MATERIALIZED length(COLUMNS('x'))
)
ENGINE = Memory;

INSERT INTO t_matcher_flatten_mat (`n.x`) VALUES ([1, 2]);
SELECT cnt FROM t_matcher_flatten_mat;

-- A matcher anchored to the un-flattened nested column name (`^n$`) matches nothing after flattening,
-- so the invalid default is rejected at CREATE time instead of silently failing on insert.
CREATE TABLE t_matcher_flatten_bad
(
    n Nested(x UInt8),
    cnt UInt64 DEFAULT length(COLUMNS('^n$'))
)
ENGINE = Memory; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE IF EXISTS t_matcher_flatten;
DROP TABLE IF EXISTS t_matcher_flatten_mat;
