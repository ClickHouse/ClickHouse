-- Tags: no-parallel, zookeeper, no-replicated-database
-- Tag no-replicated-database: distributed_ddl_output_mode is none

-- `SET DEFAULT ROLE` changes the user, so it takes an `ON CLUSTER` clause, in either position.

-- Pin the output mode so the per-host status rows of every `ON CLUSTER` statement below stay out of
-- the result regardless of how the server this runs against is configured.
SET distributed_ddl_output_mode = 'none';

DROP USER IF EXISTS 05107_user ON CLUSTER test_shard_localhost;
DROP ROLE IF EXISTS 05107_role_a ON CLUSTER test_shard_localhost;
DROP ROLE IF EXISTS 05107_role_b ON CLUSTER test_shard_localhost;

CREATE ROLE 05107_role_a ON CLUSTER test_shard_localhost;
CREATE ROLE 05107_role_b ON CLUSTER test_shard_localhost;
CREATE USER 05107_user ON CLUSTER test_shard_localhost;
GRANT ON CLUSTER test_shard_localhost 05107_role_a, 05107_role_b TO 05107_user;

SELECT 'trailing ON CLUSTER';
SET DEFAULT ROLE 05107_role_a TO 05107_user ON CLUSTER test_shard_localhost;
SELECT default_roles_all, default_roles_list FROM system.users WHERE name = '05107_user';

SELECT 'leading ON CLUSTER';
SET DEFAULT ROLE ON CLUSTER test_shard_localhost 05107_role_b TO 05107_user;
SELECT default_roles_all, default_roles_list FROM system.users WHERE name = '05107_user';

SELECT 'ALL EXCEPT';
SET DEFAULT ROLE ALL EXCEPT 05107_role_a TO 05107_user ON CLUSTER test_shard_localhost;
SELECT default_roles_all, default_roles_except FROM system.users WHERE name = '05107_user';

SELECT 'NONE';
SET DEFAULT ROLE NONE TO 05107_user ON CLUSTER test_shard_localhost;
SELECT default_roles_all, default_roles_list FROM system.users WHERE name = '05107_user';

SELECT 'formatting keeps the clause';
SELECT formatQuerySingleLine($$SET DEFAULT ROLE r TO u ON CLUSTER c$$);
SELECT formatQuerySingleLine($$SET DEFAULT ROLE ON CLUSTER c r TO u$$);

SELECT 'an unknown role is rejected on the initiator';
-- The role names are resolved before the query is distributed, so a typo is reported to the client right
-- away instead of being queued into the DDL log and failing on every host.
SET DEFAULT ROLE 05107_role_nonexistent TO 05107_user ON CLUSTER test_shard_localhost; -- { serverError UNKNOWN_ROLE }
SELECT default_roles_all, default_roles_list FROM system.users WHERE name = '05107_user';

SELECT 'SET ROLE is session-local and takes no ON CLUSTER';
SELECT formatQuerySingleLine($$SET ROLE r ON CLUSTER c$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine($$SET ROLE DEFAULT ON CLUSTER c$$); -- { serverError SYNTAX_ERROR }

DROP USER 05107_user ON CLUSTER test_shard_localhost;
DROP ROLE 05107_role_a ON CLUSTER test_shard_localhost;
DROP ROLE 05107_role_b ON CLUSTER test_shard_localhost;
