-- Tags: no-replicated-database
-- Reason: creates a Distributed table over a table function that references a session-local temporary
-- table, which does not exist on the other replicas of a Replicated database.

-- Binding a Distributed table function target to the current database at CREATE time keeps the
-- temporary/external-table exemption that plain table identifiers already have: the first argument of
-- `joinGet` names a `Join`-engine table resolved through `Context::resolveStorageID` (temporary and
-- external tables are looked up before the current database), so an unqualified name that matches a
-- session-local temporary table must NOT be rewritten to the current database - that would shadow the
-- temporary table with a permanent one of the same name.

DROP TABLE IF EXISTS dist_over_tmp_join;

CREATE TEMPORARY TABLE tmp_join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO tmp_join_src VALUES (0, 7);

-- The `joinGet` target names the temporary table, so it stays unqualified in the stored definition.
CREATE TABLE dist_over_tmp_join (n UInt64)
    ENGINE = Distributed(test_shard_localhost, numbers(joinGet('tmp_join_src', 'v', toUInt64(0))));
SHOW CREATE TABLE dist_over_tmp_join;
-- The table is intentionally not read here: a session-local temporary table only exists on the initiator, so
-- it is visible to the target table function only when the shard query runs on the local replica. Reading it
-- over the network path (a remote replica, or the parallel-replicas cluster) would raise `UNKNOWN_TABLE`.
-- Keeping the unqualified name in the stored definition (asserted above) is the property under test.

-- A short `ATTACH TABLE` reads the definition back from the table's own metadata, so it must trust the stored
-- target AST as is - not re-run the create-time normalization. Reattaching after the temporary table is gone
-- must therefore keep the target unqualified; re-binding it here would shadow it with `default.tmp_join_src`.
DETACH TABLE dist_over_tmp_join;
DROP TEMPORARY TABLE tmp_join_src;
ATTACH TABLE dist_over_tmp_join;
SHOW CREATE TABLE dist_over_tmp_join;
DROP TABLE dist_over_tmp_join;

-- Control: a permanent `Join` table of the same name IS qualified with the current database.
CREATE TABLE tmp_join_src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO tmp_join_src VALUES (0, 3);
CREATE TABLE dist_over_tmp_join (n UInt64)
    ENGINE = Distributed(test_shard_localhost, numbers(joinGet('tmp_join_src', 'v', toUInt64(0))));
SHOW CREATE TABLE dist_over_tmp_join;
SELECT count() FROM dist_over_tmp_join;
DROP TABLE dist_over_tmp_join;
DROP TABLE tmp_join_src;
