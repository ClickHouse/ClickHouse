-- Tests that MATERIALIZE TTL on a storage that is not a MergeTree, but whose metadata carries a
-- TTL, runs the mutation instead of crashing the server. Two storages qualify: an Alias table,
-- which returns its target's metadata, and a materialized view declaring a TTL on its own column.

DROP TABLE IF EXISTS alias_ttl_target;
DROP TABLE IF EXISTS alias_ttl;
DROP TABLE IF EXISTS mv_ttl_src;
DROP TABLE IF EXISTS mv_ttl;
DROP TABLE IF EXISTS alias_no_ttl_target;
DROP TABLE IF EXISTS alias_no_ttl;

CREATE TABLE alias_ttl_target (d Date, v UInt64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE alias_ttl ENGINE = Alias(alias_ttl_target);

INSERT INTO alias_ttl_target VALUES ('2000-01-01', 42);
ALTER TABLE alias_ttl MATERIALIZE TTL SETTINGS mutations_sync = 2;

CREATE TABLE mv_ttl_src (d Date, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_ttl (d Date, v UInt64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY tuple() AS SELECT d, v FROM mv_ttl_src;

INSERT INTO mv_ttl_src VALUES ('2000-01-01', 42);
-- The view's own column TTL reaches its metadata only once an alter re-applies the column list,
-- so without this statement MATERIALIZE TTL is rejected as having no TTL to materialize.
ALTER TABLE mv_ttl MODIFY COMMENT 'apply the column TTL';
ALTER TABLE mv_ttl MATERIALIZE TTL SETTINGS mutations_sync = 2;

-- One completed mutation per storage. The row value the TTL produces is deliberately not
-- asserted: a background merge applies an expired TTL too, so it would hold with no mutation.
SELECT if(table = 'alias_ttl_target', 'alias target', 'view inner table') AS storage, is_done
FROM system.mutations
WHERE database = currentDatabase() AND command LIKE '%MATERIALIZE TTL%'
ORDER BY storage;

CREATE TABLE alias_no_ttl_target (d Date, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE alias_no_ttl ENGINE = Alias(alias_no_ttl_target);
ALTER TABLE alias_no_ttl MATERIALIZE TTL; -- { serverError INCORRECT_QUERY }

DROP TABLE alias_ttl;
DROP TABLE alias_ttl_target;
DROP TABLE mv_ttl;
DROP TABLE mv_ttl_src;
DROP TABLE alias_no_ttl;
DROP TABLE alias_no_ttl_target;
