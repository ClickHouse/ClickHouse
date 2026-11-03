-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the affected rows are counted by each replica of `ReplicatedMergeTree` for the parts it mutates itself

-- Tests for `affected_rows` in `system.mutations` for `ReplicatedMergeTree`.
-- Each replica counts the rows of the parts it mutates itself, so both replicas report the same counts.
-- A replica that fetches a mutated part from another replica does not count its rows.
-- The counts are tracked in memory and are reset when the table is reloaded.
-- The rows of `system.mutations` are not sorted: for a table, they are returned in the order of mutations and commands.

-- 1. `DELETE`, both replicas mutate the part
DROP TABLE IF EXISTS t_delete_r1 SYNC;
DROP TABLE IF EXISTS t_delete_r2 SYNC;
CREATE TABLE t_delete_r1 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete', 'r1') ORDER BY id;
CREATE TABLE t_delete_r2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete', 'r2') ORDER BY id;
INSERT INTO t_delete_r1 SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_delete_r2;
ALTER TABLE t_delete_r1 DELETE WHERE id < 3 SETTINGS mutations_sync = 2;
SELECT 'delete', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_r1';
SELECT 'delete', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_r2';
DROP TABLE t_delete_r1 SYNC;
DROP TABLE t_delete_r2 SYNC;

-- 2. `UPDATE`, both replicas mutate the part
DROP TABLE IF EXISTS t_update_r1 SYNC;
DROP TABLE IF EXISTS t_update_r2 SYNC;
CREATE TABLE t_update_r1 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update', 'r1') ORDER BY id;
CREATE TABLE t_update_r2 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_update', 'r2') ORDER BY id;
INSERT INTO t_update_r1 (id) SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_update_r2;
ALTER TABLE t_update_r1 UPDATE v = 1 WHERE id < 4 SETTINGS mutations_sync = 2;
SELECT 'update', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_r1';
SELECT 'update', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_update_r2';
DROP TABLE t_update_r1 SYNC;
DROP TABLE t_update_r2 SYNC;

-- 3. `DELETE` and then `UPDATE` in one mutation, the update counts only the rows that are not deleted
DROP TABLE IF EXISTS t_delete_update_r1 SYNC;
DROP TABLE IF EXISTS t_delete_update_r2 SYNC;
CREATE TABLE t_delete_update_r1 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete_update', 'r1') ORDER BY id;
CREATE TABLE t_delete_update_r2 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_delete_update', 'r2') ORDER BY id;
INSERT INTO t_delete_update_r1 (id) SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_delete_update_r2;
ALTER TABLE t_delete_update_r1 DELETE WHERE id < 3, UPDATE v = 1 WHERE id < 5 SETTINGS mutations_sync = 2;
SELECT 'delete then update', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_update_r1';
SELECT 'delete then update', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_delete_update_r2';
DROP TABLE t_delete_update_r1 SYNC;
DROP TABLE t_delete_update_r2 SYNC;

-- 4. The same as 3, but in several mutations applied together, and a no-op mutation `DELETE WHERE 0` waits for them
DROP TABLE IF EXISTS t_batch_r1 SYNC;
DROP TABLE IF EXISTS t_batch_r2 SYNC;
CREATE TABLE t_batch_r1 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_batch', 'r1') ORDER BY id;
CREATE TABLE t_batch_r2 (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_batch', 'r2') ORDER BY id;
INSERT INTO t_batch_r1 (id) SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_batch_r2;
SYSTEM STOP MERGES t_batch_r1;
SYSTEM STOP MERGES t_batch_r2;
ALTER TABLE t_batch_r1 DELETE WHERE id < 3;
ALTER TABLE t_batch_r1 UPDATE v = 1 WHERE id < 5;
SYSTEM START MERGES t_batch_r1;
SYSTEM START MERGES t_batch_r2;
ALTER TABLE t_batch_r1 DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'batch delete then update', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_r1';
SELECT 'batch delete then update', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_batch_r2';
DROP TABLE t_batch_r1 SYNC;
DROP TABLE t_batch_r2 SYNC;

-- 5. The second replica fetches the mutated part instead of mutating it, so it does not count the rows
DROP TABLE IF EXISTS t_fetch_r1 SYNC;
DROP TABLE IF EXISTS t_fetch_r2 SYNC;
CREATE TABLE t_fetch_r1 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch', 'r1') ORDER BY id
    SETTINGS always_fetch_mutated_part = 0;
CREATE TABLE t_fetch_r2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_fetch', 'r2') ORDER BY id
    SETTINGS always_fetch_mutated_part = 1;
INSERT INTO t_fetch_r1 SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_fetch_r2;
ALTER TABLE t_fetch_r1 DELETE WHERE id < 3 SETTINGS mutations_sync = 2;
SELECT 'fetched part', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_fetch_r1';
SELECT 'fetched part', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_fetch_r2';
DROP TABLE t_fetch_r1 SYNC;
DROP TABLE t_fetch_r2 SYNC;

-- 6. The counts are reset when the table is reloaded, the other replica keeps them
DROP TABLE IF EXISTS t_reload_r1 SYNC;
DROP TABLE IF EXISTS t_reload_r2 SYNC;
CREATE TABLE t_reload_r1 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_reload', 'r1') ORDER BY id;
CREATE TABLE t_reload_r2 (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_reload', 'r2') ORDER BY id;
INSERT INTO t_reload_r1 SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA t_reload_r2;
ALTER TABLE t_reload_r1 DELETE WHERE id < 3 SETTINGS mutations_sync = 2;
DETACH TABLE t_reload_r1;
ATTACH TABLE t_reload_r1;
SELECT 'reloaded table', 'r1', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_reload_r1';
SELECT 'reloaded table', 'r2', affected_rows FROM system.mutations WHERE database = currentDatabase() AND table = 't_reload_r2';
DROP TABLE t_reload_r1 SYNC;
DROP TABLE t_reload_r2 SYNC;
