-- Tags: no-parallel
-- ^ creates global SQL UDFs (CREATE FUNCTION); concurrent copies of this test would clash on the names.

-- https://github.com/ClickHouse/ClickHouse/issues/102205
-- The alias of a SQL UDF call whose body is `untuple` must name the result columns `alias.field`,
-- exactly as `untuple(...) AS alias` written directly does.

SET enable_analyzer = 1;

DROP FUNCTION IF EXISTS test_scope_05136;
DROP FUNCTION IF EXISTS test_scope_nested_05136;
DROP FUNCTION IF EXISTS test_star_05136;
DROP FUNCTION IF EXISTS test_scope_inner_alias_05136;
DROP FUNCTION IF EXISTS test_scope_inner_alias_nested_05136;

CREATE FUNCTION test_scope_05136 AS (app, endpoint) -> untuple(CAST((app, endpoint) AS Tuple(app String, endpoint String)));
CREATE FUNCTION test_scope_nested_05136 AS (app, endpoint) -> test_scope_05136(app, endpoint);
CREATE FUNCTION test_star_05136 AS () -> *;
CREATE FUNCTION test_scope_inner_alias_05136 AS (app, endpoint) -> (untuple(CAST((app, endpoint) AS Tuple(app String, endpoint String))) AS innerscope);
CREATE FUNCTION test_scope_inner_alias_nested_05136 AS (app, endpoint) -> (test_scope_inner_alias_05136(app, endpoint) AS midscope);

SELECT '-- UDF with alias';
SELECT test_scope_05136('web-api', '/pay') AS scope FORMAT TSVWithNames;

SELECT '-- same expression without UDF';
SELECT untuple(CAST(('web-api', '/pay') AS Tuple(app String, endpoint String))) AS scope FORMAT TSVWithNames;

SELECT '-- nested UDF with alias';
SELECT test_scope_nested_05136('web-api', '/pay') AS scope FORMAT TSVWithNames;

SELECT '-- WITH lambda with alias';
WITH (a, e) -> untuple(CAST((a, e) AS Tuple(app String, endpoint String))) AS lambda_scope
SELECT lambda_scope('web-api', '/pay') AS scope FORMAT TSVWithNames;

SELECT '-- result columns are addressable by the aliased names';
SELECT scope.app, scope.endpoint FROM (SELECT test_scope_05136('web-api', '/pay') AS scope) FORMAT TSVWithNames;

SELECT '-- alias on a column, UDF over a table';
SELECT test_scope_05136(a, e) AS scope FROM (SELECT 'web-api' AS a, '/pay' AS e UNION ALL SELECT 'cli', '/login') ORDER BY 1 FORMAT TSVWithNames;

SELECT '-- untuple nested inside the body keeps its own naming';
SELECT tuple(test_scope_05136('web-api', '/pay')) AS t FORMAT TSVWithNames;

SELECT '-- alias inside the UDF body is used when the call has none';
SELECT test_scope_inner_alias_05136('web-api', '/pay') FORMAT TSVWithNames;

SELECT '-- alias of the call takes priority over the alias inside the body';
SELECT test_scope_inner_alias_05136('web-api', '/pay') AS scope FORMAT TSVWithNames;

SELECT '-- the outermost alias wins across nested UDFs';
SELECT test_scope_inner_alias_nested_05136('web-api', '/pay') FORMAT TSVWithNames;
SELECT test_scope_inner_alias_nested_05136('web-api', '/pay') AS scope FORMAT TSVWithNames;

SELECT '-- matcher body ignores the alias';
SELECT test_star_05136() AS s FROM (SELECT 1 AS a, 2 AS b) FORMAT TSVWithNames;

DROP FUNCTION test_scope_05136;
DROP FUNCTION test_scope_nested_05136;
DROP FUNCTION test_star_05136;
DROP FUNCTION test_scope_inner_alias_05136;
DROP FUNCTION test_scope_inner_alias_nested_05136;
