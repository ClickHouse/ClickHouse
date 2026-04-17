-- https://github.com/ClickHouse/ClickHouse/issues/92067
CREATE TABLE t0 (c0 Int) ENGINE = Memory;
SET enable_scopes_for_with_statement = 0, enable_analyzer = 1;
INSERT INTO TABLE t0 (c0) WITH (SELECT 1) AS a0 SELECT (SELECT 2);
SELECT * FROM t0;
