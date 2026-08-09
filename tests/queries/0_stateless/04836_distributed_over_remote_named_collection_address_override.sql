-- Tags: no-parallel
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent tests
-- (see 02918_fuzzjson_table_function.sql for the same requirement), and the flaky check runs this very
-- test concurrently with itself, so one run's DROP NAMED COLLECTION would remove the collection from
-- under another run.

-- In the named-collection form of `remote` / `remoteSecure` a literal `addresses_expr` / `host` /
-- `hostname` / `port` override replaces the collection's own address at read time
-- (`tryGetNamedCollectionWithOverrides` merges it over the collection), so the locality of the target -
-- and with it the referential dependency of a persisted table on a local source - must be decided from
-- the merged view. A collection pointing elsewhere but overridden to `127.0.0.1` reads the local table,
-- so the dependency must be recorded (without it, `DROP` / `RENAME` of the source would be allowed under
-- `check_referential_table_dependencies = 1` even though the persisted table then fails at read time);
-- conversely, a local collection overridden to a non-local address must not pin the source down.
DROP NAMED COLLECTION IF EXISTS nc_04836_nonlocal;
DROP NAMED COLLECTION IF EXISTS nc_04836_nonlocal_host;
DROP NAMED COLLECTION IF EXISTS nc_04836_local;
CREATE NAMED COLLECTION nc_04836_nonlocal AS addresses_expr = '127.0.0.2', database = '', table = 'dep_src';
CREATE NAMED COLLECTION nc_04836_nonlocal_host AS host = '127.0.0.2', database = '', table = 'dep_src';
CREATE NAMED COLLECTION nc_04836_local AS addresses_expr = '127.0.0.1', database = '', table = 'dep_src';
CREATE TABLE dep_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO dep_src VALUES (1), (2), (3);

CREATE TABLE dist_override_addresses ENGINE = Distributed(test_shard_localhost, remote(nc_04836_nonlocal, addresses_expr = '127.0.0.1', database = currentDatabase(), table = 'dep_src'));
CREATE TABLE dist_override_host ENGINE = Distributed(test_shard_localhost, remote(nc_04836_nonlocal_host, host = '127.0.0.1', database = currentDatabase()));
CREATE TABLE dist_override_nonlocal ENGINE = Distributed(test_shard_localhost, remote(nc_04836_local, addresses_expr = '127.0.0.2'));

-- A numeric `port` override merges the same way. `tcpPort()` is a server constant, which the initiator
-- of a distributed query deliberately does not fold (it is to be evaluated per shard), so this target is
-- not readable through the persisted table - the structure is declared to keep the CREATE from resolving
-- it - but the dependency analysis evaluates the override with the global context and must still record
-- the dependency on the local source.
CREATE TABLE dist_override_host_port (n UInt64) ENGINE = Distributed(test_shard_localhost, remote(nc_04836_nonlocal_host, host = '127.0.0.1', port = tcpPort(), database = currentDatabase()));

SELECT sum(n) FROM dist_override_addresses;
SELECT sum(n) FROM dist_override_host;

-- Dropping the dependent tables one by one shows each locally-overridden table holds its own referential
-- dependency on `dep_src`; once only the non-locally-overridden one is left, the drop is allowed.
SET check_referential_table_dependencies = 1;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_override_addresses;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_override_host;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_override_host_port;
DROP TABLE dep_src;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_override_nonlocal;
DROP NAMED COLLECTION nc_04836_nonlocal;
DROP NAMED COLLECTION nc_04836_nonlocal_host;
DROP NAMED COLLECTION nc_04836_local;
