-- A full-definition `ATTACH TABLE t (...) ENGINE = ...` is CREATE-like user input, so stored
-- default expressions get the same validation as in `CREATE TABLE`: a definition `CREATE TABLE`
-- rejects must not be persistable through `ATTACH TABLE`, or it would fail only on a later
-- `INSERT` or `ALTER`.

SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
-- Full-definition ATTACH requires UUID with Atomic, so use an Ordinary database.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Alias-lambda capture: `y` expands to `x + 1` inside `arrayMap(x -> ...)`.
ATTACH TABLE t_attach_capture
(
    x UInt8,
    arr Array(UInt8),
    y UInt8 ALIAS x + 1,
    m Array(UInt8) MATERIALIZED arrayMap(x -> y, arr)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Cyclic defaults.
ATTACH TABLE t_attach_cycle
(
    a UInt8 DEFAULT b + 1,
    b UInt8 DEFAULT a + 1
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError CYCLIC_ALIASES }

-- A default over a virtual column is rejected only for a definition created now: a table created by
-- an affected version already carries one, and re-stating its definition through a full-definition
-- `ATTACH TABLE` must keep loading it.
ATTACH TABLE t_attach_virtual
(
    c0 String MATERIALIZED _table,
    c1 UInt16
) ENGINE = MergeTree ORDER BY tuple();

SELECT 'legacy attach over a virtual column ok';
DROP TABLE t_attach_virtual;

-- A valid full definition still attaches and works like CREATE.
ATTACH TABLE t_attach_ok
(
    a UInt8,
    m UInt8 MATERIALIZED a + 1
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_attach_ok (a) VALUES (1);
SELECT a, m FROM t_attach_ok;

-- Short ATTACH of previously stored metadata stays tolerant of it.
DETACH TABLE t_attach_ok;
ATTACH TABLE t_attach_ok;
SELECT a, m FROM t_attach_ok;

DROP TABLE t_attach_ok;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
