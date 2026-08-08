-- A `remote()` / `remoteSecure()` target that names this server runs locally, so a persisted table over
-- it depends on the local table the function reads: without a recorded referential dependency,
-- `DROP` / `RENAME` of that table would be allowed under `check_referential_table_dependencies = 1` even
-- though the persisted table then fails at read time. The address pattern cannot be attributed to this
-- server in full generality (an arbitrary host name would need a DNS lookup, which the dependency
-- analysis cannot afford at server startup), but the spellings that need no resolution - an IP literal or
-- `localhost` on the server's own port - are recognized by `DDLDependencyVisitor`.
CREATE TABLE dep_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO dep_src VALUES (1), (2), (3);

-- The loop-back form records a referential dependency on the local table it reads, both as the target of
-- a `Distributed` table and as a direct `CREATE TABLE ... AS remote(...)` target. An empty database
-- argument is bound to the creating database at CREATE time, so it yields the same dependency.
-- 127.0.0.2 reaches this server in the test environment, but it is not a local address (`isLocalAddress`
-- accepts only 127.{0,1}.{0,1}.{0,1} of the loop-back range), so - like any pattern that cannot be
-- attributed to this server - it records no dependency.
CREATE TABLE dist_local_remote ENGINE = Distributed(test_shard_localhost, remote('127.0.0.1', currentDatabase(), 'dep_src'));
CREATE TABLE dist_local_remote_empty_db ENGINE = Distributed(test_shard_localhost, remote('127.0.0.1', '', 'dep_src'));
CREATE TABLE direct_local_remote AS remote('127.0.0.1', currentDatabase(), 'dep_src');
CREATE TABLE dist_nonlocal_remote ENGINE = Distributed(test_shard_localhost, remote('127.0.0.2', currentDatabase(), 'dep_src'));

SELECT sum(n) FROM dist_local_remote;

-- Dropping the three dependent tables one by one shows each of them holds its own referential dependency
-- on `dep_src`; once only the non-local one is left, the drop is allowed.
SET check_referential_table_dependencies = 1;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_local_remote;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE dist_local_remote_empty_db;
DROP TABLE dep_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE direct_local_remote;
DROP TABLE dep_src;
SET check_referential_table_dependencies = 0;
DROP TABLE dist_nonlocal_remote;
