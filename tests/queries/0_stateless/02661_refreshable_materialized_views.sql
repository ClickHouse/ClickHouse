CREATE MATERIALIZED VIEW test REFRESH AFTER 15 SECOND ENGINE = MergeTree() ORDER BY number AS SELECT * FROM system.numbers LIMIT 10000000;

SELECT view FROM system.view_refreshes WHERE view = 'test';

CREATE MATERIALIZED VIEW test1 REFRESH EVERY 1 HOUR ENGINE = MergeTree() ORDER BY number AS SELECT * FROM test;

CREATE MATERIALIZED VIEW test2 REFRESH EVERY 2 HOUR OFFSET 42 MINUTE 8 SECOND RANDOMIZE FOR 10 MINUTE ENGINE = MergeTree() ORDER BY number AS SELECT * FROM test;

SELECT view FROM system.view_refreshes WHERE view LIKE 'test%' ORDER BY view;

SYSTEM STOP VIEW test;
SYSTEM STOP VIEWS;

SELECT view, refresh_status FROM system.view_refreshes WHERE view LIKE 'test%' ORDER BY view;

SYSTEM START VIEWS;

DROP VIEW test;
DROP VIEW test1;
DROP VIEW test2;



create materialized view b refresh every -1 second (x Int64) engine Memory as select 1; -- { clientError SYNTAX_ERROR }
create materialized view b refresh every 100 millisecond (x Int64) engine Memory as select 1; -- { clientError BAD_ARGUMENTS }
create materialized view b refresh every 1 month offset 28 day (x Int64) engine Memory as select 1; -- { clientError BAD_ARGUMENTS }

-- TODO: basic scheduling, common schedules like EVERY 1 MONTH, EVERY 2 WEEK OFFSET 3, etc
-- TODO: select from table function
-- TODO: UNION ALL
-- TODO: DEPENDS, with AFTER and with EVERY
-- TODO: RANDOMIZE FOR
-- TODO: exceptions
-- TODO: progress indication
-- TODO: dropping during refresh
-- TODO: pause, resume, cancel, refresh, stop, start, stop all, start all
-- TODO: RENAME
-- TODO: ALTER MODIFY REFRESH (schedule, dependencies, settings)
-- TODO: ALTER MODIFY QUERY (with allow_experimental_alter_materialized_view_structure)
-- TODO: ALTER columns, with and without also altering query
-- TODO: refresh SETTINGS
-- TODO: query SETTIGNS
-- TODO: refusing in non-atomic databases
-- TODO: concurrency limits
-- TODO: stop_refreshable_materialized_views_on_startup
