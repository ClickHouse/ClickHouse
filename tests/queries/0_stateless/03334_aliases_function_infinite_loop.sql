-- Tags: no-parallel
-- ^ creates a function
SET enable_analyzer = 1;
DROP FUNCTION IF EXISTS f0;
DROP VIEW IF EXISTS v0;
CREATE FUNCTION f0 AS (x) -> toInt32((x AS c0) % 2 AS c1);
CREATE VIEW v0 AS (SELECT 0 AS c0, c0 AS c1, f0(c1) AS c2); -- { serverError UNKNOWN_IDENTIFIER }
DROP FUNCTION f0;
