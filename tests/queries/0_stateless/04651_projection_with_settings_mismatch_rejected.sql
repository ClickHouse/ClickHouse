-- Projection definitions are compared as ASTs rather than as formatted text. A `WITH SETTINGS`
-- clause is part of a projection definition, so projections that differ only inside that clause
-- must still be rejected, while definitions that differ only in formatting must still be accepted.

DROP TABLE IF EXISTS t_proj_a;
DROP TABLE IF EXISTS t_proj_b;

-- Two settings reset to their default value. `x = DEFAULT` is recorded in `default_settings`
-- rather than in `changes`, so hashing only `changes` would make these two definitions equal.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (max_compress_block_size = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- A setting reset to its default value versus the same setting given an explicit value.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = 42))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- A reset setting versus no `WITH SETTINGS` clause at all.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- Query parameters. `param_x = ...` is recorded in `query_parameters` rather than in `changes`.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (param_x = 1))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (param_x = 2))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- A differing parameter name, same value.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (param_x = 1))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (param_y = 1))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- A setting whose value happens to spell out the bytes of a query parameter of the same name.
-- The parser strips the `param_` prefix, so without length-prefixed and counted carriers these
-- two definitions stream identical bytes.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (max_compress_block_size = 3458764513820540928))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (param_max_compress_block_size = 0))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;

-- Two resets of the same setting differing only in formatting must still be accepted.
CREATE TABLE t_proj_a (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_proj_b (a UInt32, b UInt32,
    PROJECTION p (SELECT a, b ORDER BY (b)) WITH SETTINGS (index_granularity = DEFAULT))
ENGINE = MergeTree PARTITION BY a ORDER BY a;
INSERT INTO t_proj_b VALUES (1, 10);
ALTER TABLE t_proj_a REPLACE PARTITION 1 FROM t_proj_b;
SELECT count() FROM t_proj_a;
DROP TABLE t_proj_a;
DROP TABLE t_proj_b;
