-- Tags: no-parallel, no-old-analyzer
-- no-parallel: a settings profile is server-global rather than per-database, and its name cannot be
-- made unique per run: query parameters are not accepted in access-entity DDL. So this test is not
-- safe against a concurrent copy of itself - which is how the flaky check runs it.
-- no-old-analyzer: the clamp is analyzer-path behaviour; the legacy interpreter has always thrown
-- on a nested clause that violates the constraints, so these expectations do not hold there.

-- A `SETTINGS` clause nested inside a subquery, a CTE, or a view's inner query must not override the
-- session's settings constraints. The nested form is clamped rather than rejected: a change that
-- violates a `CONST` or `readonly` constraint is dropped, a value outside its `MIN`/`MAX` bounds is
-- clamped into them, and the rest of the clause applies. A clause on the outer query still throws.

DROP SETTINGS PROFILE IF EXISTS profile_05019;
DROP VIEW IF EXISTS v_invoker_05019;
DROP VIEW IF EXISTS v_definer_05019;
DROP TABLE IF EXISTS t_05019;

CREATE TABLE t_05019 (tenant_id UInt32, secret String) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO t_05019 VALUES (1, 'tenant1-own'), (2, 'tenant2-secret'), (3, 'tenant3-secret');

-- Views created by an unconstrained user, carrying an inner SETTINGS clause the constrained session
-- below is not allowed to set. Under SQL SECURITY INVOKER the inner clause is clamped against the
-- invoking session's constraints, so the view keeps reading with the constraints enforced; under
-- DEFINER the definer's own (unconstrained) settings apply, as before.
CREATE VIEW v_invoker_05019 SQL SECURITY INVOKER
    AS SELECT count() AS c FROM t_05019 SETTINGS max_execution_time = 5;
CREATE VIEW v_definer_05019 DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT count() AS c FROM numbers(3) SETTINGS max_execution_time = 5;

CREATE SETTINGS PROFILE profile_05019 SETTINGS
    max_execution_time = 10 CONST,
    additional_table_filters = '{''t_05019'':''tenant_id = 1''}' CONST,
    max_rows_to_read MAX 2;

SET profile = 'profile_05019';

SELECT '-- the row filter installed by the profile is in force';
SELECT count() FROM t_05019;

SELECT '-- a nested clause cannot override a CONST setting: the row filter stays, in a subquery and in a CTE';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'});
WITH cte AS (SELECT * FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'}) SELECT count() FROM cte;

-- A nested compound query carries the clause on one of its member SELECTs, whichever one the parser
-- attached it to, so every member has to be clamped and not just the query as a whole.
SELECT '-- every member of a nested compound query is clamped';
SELECT count() FROM (SELECT tenant_id FROM t_05019 UNION ALL SELECT tenant_id FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'});
SELECT count() FROM ((SELECT tenant_id FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'}) UNION ALL SELECT tenant_id FROM t_05019);
SELECT count() FROM (SELECT tenant_id FROM t_05019 INTERSECT SELECT tenant_id FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'});

-- Reading 3 rows under `max_rows_to_read MAX 2`: clamped into the bound the read fails, so the error
-- proves the value was clamped - applied as written (100) or dropped (unlimited) it would succeed.
SELECT '-- a nested value above MAX is clamped into the bound, not applied as written';
SELECT sum(number) FROM (SELECT number FROM numbers(3) SETTINGS max_rows_to_read = 100); -- { serverError TOO_MANY_ROWS }

SELECT '-- a nested value inside the bounds still applies';
SELECT sum(number) FROM (SELECT number FROM numbers(3) SETTINGS max_rows_to_read = 1); -- { serverError TOO_MANY_ROWS }

SELECT '-- clamping still lets the subquery read up to the bound';
SELECT sum(number) FROM (SELECT number FROM numbers(2) SETTINGS max_rows_to_read = 100);

SELECT '-- an unconstrained nested clause still works';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS max_block_size = 100);

SELECT '-- the session settings are untouched by a nested clause';
SELECT getSetting('max_execution_time');

-- Applying a nested reset needs a `default_settings` carrier on `QueryNode`; until then it stays
-- ignored, as it always was on this path. Tracked in issue #115415.
SELECT '-- a nested SETTINGS name = DEFAULT is still ignored';
SELECT count() FROM (SELECT * FROM t_05019 SETTINGS max_execution_time = DEFAULT);
SELECT getSetting('max_execution_time');

SELECT '-- a SQL SECURITY INVOKER view with an inner clause the invoker may not set reads with the constraints enforced';
SELECT c FROM v_invoker_05019;

SELECT '-- a SQL SECURITY DEFINER view still reads';
SELECT c FROM v_definer_05019;

-- Kept last on purpose. A constraint violation on a top-level SETTINGS clause is raised while the
-- server receives the settings packet, so the connection is dropped and the client silently
-- reconnects into a fresh session, losing the `SET profile` above for anything that follows.
SELECT '-- a top-level SETTINGS clause overriding a CONST setting still throws';
SELECT count() FROM t_05019 SETTINGS additional_table_filters = {'t_05019':'1'}; -- { serverError SETTING_CONSTRAINT_VIOLATION }

DROP TABLE t_05019;
DROP VIEW v_invoker_05019;
DROP VIEW v_definer_05019;
DROP SETTINGS PROFILE profile_05019;
