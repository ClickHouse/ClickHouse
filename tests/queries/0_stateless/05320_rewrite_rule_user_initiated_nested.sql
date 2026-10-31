-- Tags: no-parallel
-- no-parallel: rewrite rules are global server state

-- The subqueries of `PARALLEL WITH` are executed as nested (`internal`) queries, but their SQL is
-- written by the user (`user_initiated`), so the active rewrite rules must apply to them just as
-- if they were submitted directly. Otherwise wrapping a statement into `PARALLEL WITH` would
-- bypass a `REJECT` rule.

DROP RULE IF EXISTS rule_05320;
CREATE RULE rule_05320 AS (DROP TABLE IF EXISTS t_05320_blocked) REJECT WITH 'blocked';

SET query_rules = 'rule_05320';
DROP TABLE IF EXISTS t_05320_blocked; -- { serverError REWRITE_RULE_REJECTION }
DROP TABLE IF EXISTS t_05320_other PARALLEL WITH DROP TABLE IF EXISTS t_05320_blocked; -- { serverError REWRITE_RULE_REJECTION }
DROP TABLE IF EXISTS t_05320_other PARALLEL WITH DROP TABLE IF EXISTS t_05320_other_2;
SELECT 'ok';

SET query_rules = '';
DROP RULE rule_05320;
