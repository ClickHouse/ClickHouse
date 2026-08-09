-- Tags: no-parallel
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent tests
-- (see 02918_fuzzjson_table_function.sql for the same requirement), and the flaky check runs this very
-- test concurrently with itself, so one run's DROP NAMED COLLECTION would remove the collection from
-- under another run.

-- The first argument of `remote` / `remoteSecure` accepts more local spellings than an inline address
-- pattern (04824_distributed_over_local_remote_dependencies): a bare identifier names either a named
-- collection or a configured cluster. When the resolved cluster or collection points back to this
-- server, a persisted target reads the underlying table locally, so it must record a referential
-- dependency on it - without one, `DROP` / `RENAME` of that table would be allowed under
-- `check_referential_table_dependencies = 1` even though the persisted table then fails at read time.
-- For the named-collection form the collection stores the addresses / database / table, each replaceable
-- by a `key = value` override, and an empty stored database has the create-time database injected as an
-- override at CREATE time (04826_distributed_over_remote_named_collection_internal_database), so the
-- dependency lands on the creating database. A collection pointing to 127.0.0.2 reaches this server in
-- the test environment but is not a local address (`isLocalAddress` accepts only 127.{0,1}.{0,1}.{0,1}),
-- so - like any pattern that cannot be attributed to this server - it records no dependency.
DROP NAMED COLLECTION IF EXISTS nc_04827_local;
DROP NAMED COLLECTION IF EXISTS nc_04827_nonlocal;
CREATE NAMED COLLECTION nc_04827_local AS addresses_expr = '127.0.0.1', database = '', table = 'dep_src';
CREATE NAMED COLLECTION nc_04827_nonlocal AS addresses_expr = '127.0.0.2', database = '', table = 'dep_src';
CREATE TABLE dep_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO dep_src VALUES (1), (2), (3);

CREATE TABLE dist_cluster_name ENGINE = Distributed(test_shard_localhost, remote(test_shard_localhost, currentDatabase(), 'dep_src'));
CREATE TABLE dist_nc ENGINE = Distributed(test_shard_localhost, remote(nc_04827_local));
CREATE TABLE dist_nc_table_override ENGINE = Distributed(test_shard_localhost, remote(nc_04827_local, table = 'dep_src'));
CREATE TABLE dist_nc_nonlocal ENGINE = Distributed(test_shard_localhost, remote(nc_04827_nonlocal));

SELECT sum(n) FROM dist_cluster_name;
SELECT sum(n) FROM dist_nc;

-- Dropping the dependent tables one by one shows each of them holds its own referential dependency on
-- `dep_src`; once only the non-local one is left, the drop is allowed.
SET check_referential_table_dependencies = 1;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_cluster_name;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_nc;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_nc_table_override;
DROP TABLE dep_src;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_nc_nonlocal;
DROP NAMED COLLECTION nc_04827_nonlocal;
DROP NAMED COLLECTION nc_04827_local;
