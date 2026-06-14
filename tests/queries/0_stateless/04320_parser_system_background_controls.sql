-- Exercise the SYSTEM query parser and AST formatter for the engine-agnostic
-- background-control commands (STOP/START/PAUSE/CANCEL/REFRESH [db.]table and
-- ... ALL BACKGROUND). The final two sections pin that these verbs reject
-- ON CLUSTER (like the SYSTEM ... VIEW aliases they mirror) and apply only to
-- tables with controllable background activity.

SELECT '--- per-table forms (db.table and bare table) ---';
EXPLAIN SYNTAX SYSTEM STOP db.t;
EXPLAIN SYNTAX SYSTEM START db.t;
EXPLAIN SYNTAX SYSTEM PAUSE db.t;
EXPLAIN SYNTAX SYSTEM CANCEL db.t;
EXPLAIN SYNTAX SYSTEM REFRESH db.t;
EXPLAIN SYNTAX SYSTEM STOP t;
EXPLAIN SYNTAX SYSTEM START t;
EXPLAIN SYNTAX SYSTEM PAUSE t;
EXPLAIN SYNTAX SYSTEM CANCEL t;
EXPLAIN SYNTAX SYSTEM REFRESH t;

SELECT '--- ALL BACKGROUND forms ---';
EXPLAIN SYNTAX SYSTEM STOP ALL BACKGROUND;
EXPLAIN SYNTAX SYSTEM START ALL BACKGROUND;
EXPLAIN SYNTAX SYSTEM PAUSE ALL BACKGROUND;
EXPLAIN SYNTAX SYSTEM CANCEL ALL BACKGROUND;
EXPLAIN SYNTAX SYSTEM REFRESH ALL BACKGROUND;

SELECT '--- the bare verbs must not shadow the existing keyword forms ---';
EXPLAIN SYNTAX SYSTEM STOP MERGES db.t;
EXPLAIN SYNTAX SYSTEM START MERGES db.t;
EXPLAIN SYNTAX SYSTEM STOP VIEW db.v;
EXPLAIN SYNTAX SYSTEM START VIEW db.v;
EXPLAIN SYNTAX SYSTEM PAUSE VIEW db.v;
EXPLAIN SYNTAX SYSTEM CANCEL VIEW db.v;
EXPLAIN SYNTAX SYSTEM REFRESH VIEW db.v;
EXPLAIN SYNTAX SYSTEM STOP VIEWS;
EXPLAIN SYNTAX SYSTEM START VIEWS;
EXPLAIN SYNTAX SYSTEM PAUSE VIEWS;

SELECT '--- table names that collide with command keywords need backticks ---';
EXPLAIN SYNTAX SYSTEM STOP db.background;
EXPLAIN SYNTAX SYSTEM STOP all;

SELECT '--- ON CLUSTER is not supported (matches the SYSTEM ... VIEW aliases) ---';
SYSTEM STOP db.t ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM START db.t ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM PAUSE db.t ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM CANCEL db.t ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM REFRESH db.t ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM STOP ALL BACKGROUND ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM START ALL BACKGROUND ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM PAUSE ALL BACKGROUND ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM CANCEL ALL BACKGROUND ON CLUSTER c; -- { clientError SYNTAX_ERROR }
SYSTEM REFRESH ALL BACKGROUND ON CLUSTER c; -- { clientError SYNTAX_ERROR }

SELECT '--- only tables with controllable background activity are accepted ---';
create table mt (x Int64) engine MergeTree order by x;
system stop mt;    -- { serverError BAD_ARGUMENTS }
system start mt;   -- { serverError BAD_ARGUMENTS }
system pause mt;   -- { serverError BAD_ARGUMENTS }
system cancel mt;  -- { serverError BAD_ARGUMENTS }
system refresh mt; -- { serverError BAD_ARGUMENTS }
-- A non-refreshable materialized view also has no controllable background activity.
create materialized view mv (x Int64) engine MergeTree order by x as select x from mt;
system stop mv;    -- { serverError BAD_ARGUMENTS }
system refresh mv; -- { serverError BAD_ARGUMENTS }
drop table mv;
drop table mt;
