-- https://github.com/ClickHouse/ClickHouse/issues/23194
SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tbl
(
    col String,
    {CLICKHOUSE_DATABASE:Identifier} Nested
    (
        tbl Nested
        (
            col String
        )
    )
)
ENGINE = Memory;

SELECT {CLICKHOUSE_DATABASE:Identifier}.tbl.col FROM {CLICKHOUSE_DATABASE:Identifier}.tbl;


SELECT {CLICKHOUSE_DATABASE:Identifier}.* FROM tbl;
SELECT {CLICKHOUSE_DATABASE:Identifier} FROM tbl;


SELECT * FROM tbl;
SELECT count(*) FROM tbl;
SELECT * + * FROM VALUES('a UInt16', 1, 10);

SELECT '---';

SELECT * GROUP BY *;
-- not ok as every component of ORDER BY may contain ASC/DESC and COLLATE; though can be supported in some sense
-- but it works
SELECT * ORDER BY *;
SELECT * WHERE *;  -- { serverError UNSUPPORTED_METHOD }

SELECT '---';

SELECT * FROM (SELECT 1 AS a) AS t, (SELECT 2 AS b) AS u;
-- equivalent to:
SELECT a, b FROM (SELECT 1 AS a) AS t, (SELECT 2 AS b) AS u;

SELECT '---';

SELECT * FROM (SELECT 1 AS a) AS t, (SELECT 1 AS a) AS u;
-- equivalent to:
SELECT t.a, u.a FROM (SELECT 1 AS a) AS t, (SELECT 1 AS a) AS u;

SELECT '---';

---- TODO: think about it
--CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t
--(
--    a UInt16
--)
--ENGINE = Memory;
--
--CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
--(
--    a UInt16
--)
--ENGINE = Memory;
--
--SELECT * FROM (SELECT 1 AS a) AS {CLICKHOUSE_DATABASE_1:Identifier}.t, (SELECT 1 AS a) AS {CLICKHOUSE_DATABASE:Identifier}.t;
---- equivalent to:
--SELECT {CLICKHOUSE_DATABASE_1:Identifier}.t.a, {CLICKHOUSE_DATABASE:Identifier}.t.a FROM (SELECT 1 AS a) AS {CLICKHOUSE_DATABASE_1:Identifier}.t, (SELECT 1 AS a) AS {CLICKHOUSE_DATABASE:Identifier}.t;


CREATE TABLE t
(
    x String,
    nest Nested
    (
        a String,
        b String
    )
) ENGINE = Memory;

SELECT * FROM t;

-- equivalent to:
SELECT x, nest.* FROM t;

-- equivalent to:
SELECT x, nest.a, nest.b FROM t;
