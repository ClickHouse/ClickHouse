-- Tags: no-parallel
-- ^ creates a user

-- Regression test: reading a materialized view whose target is a Distributed table must not crash
-- when the outer query runs on the old interpreter (enable_analyzer = 0) while the view's
-- SQL SECURITY context has the analyzer on. Previously this dereferenced a null planner context
-- in buildQueryTreeDistributed (SIGSEGV).

DROP TABLE IF EXISTS 04338_mv;
DROP TABLE IF EXISTS 04338_local;
DROP USER IF EXISTS test_definer_04338;

CREATE TABLE 04338_local (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO 04338_local SELECT number, toString(number) FROM numbers(10);

-- Definer that pins enable_analyzer = 1 with a const constraint, so the caller's enable_analyzer = 0
-- is clamped away when the SQL security overridden context is built.
CREATE USER test_definer_04338 IDENTIFIED WITH no_password SETTINGS enable_analyzer = 1 CONST;
GRANT SELECT ON *.* TO test_definer_04338;

CREATE MATERIALIZED VIEW 04338_mv (a UInt64, b String)
    ENGINE = Distributed(test_shard_localhost, currentDatabase(), 04338_local)
    DEFINER = test_definer_04338 SQL SECURITY DEFINER
    AS SELECT a, b FROM 04338_local;

-- The crashing query: outer query forced onto the old interpreter.
SELECT count(), sum(a) FROM 04338_mv SETTINGS enable_analyzer = 0;
-- And the analyzer path still works.
SELECT count(), sum(a) FROM 04338_mv SETTINGS enable_analyzer = 1;

DROP TABLE 04338_mv;
DROP TABLE 04338_local;
DROP USER test_definer_04338;
