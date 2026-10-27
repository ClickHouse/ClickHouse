-- https://github.com/ClickHouse/ClickHouse/issues/23194
SET enable_analyzer = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};
USE {CLICKHOUSE_DATABASE:Identifier};

-- simple tuple access operator
SELECT tuple(1, 'a').1;
-- named tuple or complex column access operator - can be applied to Nested type as well as Array of named Tuple
SELECT CAST(('hello', 1) AS Tuple(hello String, count UInt32)) AS t, t.hello;
-- TODO: this doesn't work
-- https://github.com/ClickHouse/ClickHouse/issues/57361
-- SELECT CAST(('hello', 1) AS Tuple(hello String, count UInt32)).hello;

-- expansion of a tuple or complex column with asterisk
SELECT tuple(1, 'a').*;

SELECT '---';

SELECT CAST(('hello', 1) AS Tuple(name String, count UInt32)).*;

SELECT untuple(CAST(('hello', 1) AS Tuple(name String, count UInt32))); -- will give two columns `name` and `count`.

SELECT '---';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t
(
    col String,
    hello String,
    world String
)
ENGINE = Memory;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.u
(
    cc String
)
ENGINE = Memory;

SELECT * EXCEPT('hello|world');
-- TODO: Qualified matcher t.* EXCEPT 'hello|world' does not find table.
-- SELECT t.* EXCEPT(hello, world);
-- SELECT {CLICKHOUSE_DATABASE:Identifier}.t.* REPLACE(x + 1 AS x);


SELECT * EXCEPT(hello) REPLACE(x + 1 AS x);

SELECT COLUMNS('^c') FROM t;
SELECT t.COLUMNS('^c') FROM t, u;
SELECT t.COLUMNS('^c') EXCEPT (test_hello, test_world) FROM t, u;

SELECT '---';

SELECT * FROM (SELECT x, x FROM (SELECT 1 AS x));
SELECT x FROM (SELECT x, x FROM (SELECT 1 AS x));
SELECT 1 FROM (SELECT x, x FROM (SELECT 1 AS x));

SELECT '---';

SELECT `plus(1, 2)` FROM (SELECT 1 + 2);

-- Lambda expressions can be aliased. (proposal)
--SELECT arrayMap(plus, [1, 2], [10, 20]);
--SELECT x -> x + 1 AS fun;

SELECT '---';

SELECT x FROM numbers(5 AS x);


SELECT '---';

CREATE TEMPORARY TABLE aliased
(
    x UInt8 DEFAULT 0,
    y ALIAS x + 1
);

INSERT INTO aliased VALUES (10);

SELECT y FROM aliased;

CREATE TEMPORARY TABLE aliased2
(
    x UInt8,
    y ALIAS ((x + 1) AS z) + 1
);

SELECT x, y, z FROM aliased2; -- { serverError UNKNOWN_IDENTIFIER }


SELECT '---';

CREATE TEMPORARY TABLE aliased3
(
    x UInt8,
    y ALIAS z + 1,
    z ALIAS x + 1
);
INSERT INTO aliased3 VALUES (10);

SELECT x, y, z FROM aliased3;
