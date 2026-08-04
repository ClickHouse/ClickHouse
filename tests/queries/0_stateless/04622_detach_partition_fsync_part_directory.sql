-- Tags: no-object-storage, no-parallel-replicas
-- no-object-storage: freeze() fsyncs local directories only; getDirectorySyncGuard() is a no-op on
--   object/remote disks, so DirectorySync is legitimately 0 there.
-- no-parallel-replicas: the DirectorySync counts are read from system.part_log; with parallel replicas
--   the runner points cluster_for_parallel_replicas at a cluster absent on the single-node test server.
-- Settings randomization stays enabled. Only min_bytes_for_full_part_storage is pinned per table
--   below so each table's storage type is deterministic: the first group pins it to 0 (its default)
--   to keep parts in Full (directory) storage, and the second group pins it high to force Packed
--   (single data.packed archive) storage. Both storage types clone the part into a detached/ *directory*
--   during DETACH and must fsync that directory subtree + ancestor chain; the packed group is the
--   regression guard for DataPartStorageOnDiskPacked::freeze, whose freeze override is independent of
--   the Full one. Every other setting is left randomized.

-- ALTER TABLE ... DETACH PARTITION must fsync the detached/ clone's directories when
-- fsync_part_directory is set, otherwise a power loss right after the acknowledgement can erase the
-- clone (issue #111382): the covering empty part that removes the rows from the active set IS fsynced,
-- so on btrfs the un-synced clone dentries roll back and the only copy of the detached data is destroyed.
-- The directory fsyncs are observed via ProfileEvents['DirectorySync'] on the covering empty part's
-- NewPart row in system.part_log (part_log ProfileEvents are a synchronous snapshot taken on the query
-- thread, unlike the per-query query_log counters which under CI load do not reliably capture the
-- clone-freeze fsyncs run in a nested profile-events scope).
--
-- The signal is relative: a DROP fsyncs only the covering empty part, while a DETACH additionally fsyncs
-- the clone leaf directory and walks its ancestor chain up to the disk root, so a DETACH issues at least
-- two more directory fsyncs than a DROP (clone leaf plus at least one ancestor), and a projection adds one
-- more clone subdirectory. Comparing counts keeps the test independent of the ancestor path depth.

DROP TABLE IF EXISTS detach_fsync_on;
DROP TABLE IF EXISTS detach_fsync_proj;
DROP TABLE IF EXISTS detach_fsync_drop;
DROP TABLE IF EXISTS detach_fsync_off;

CREATE TABLE detach_fsync_on   (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 0;
CREATE TABLE detach_fsync_proj (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v)) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 0;
CREATE TABLE detach_fsync_drop (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 0;
CREATE TABLE detach_fsync_off  (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 0, storage_policy = 'default', min_bytes_for_full_part_storage = 0;

INSERT INTO detach_fsync_on   SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_proj SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_drop SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_off  SELECT number, number % 10 FROM numbers(1000);

-- Each ALTER commits one covering empty part named all_1_1_1 (the INSERT part is all_1_1_0).
ALTER TABLE detach_fsync_on   DETACH PARTITION tuple();
ALTER TABLE detach_fsync_proj DETACH PARTITION tuple();
ALTER TABLE detach_fsync_drop DROP   PARTITION tuple();
ALTER TABLE detach_fsync_off  DETACH PARTITION tuple();

SYSTEM FLUSH LOGS part_log;

-- DirectorySync on the covering empty part of each ALTER. DETACH fsyncs the clone subtree + ancestor
-- chain on top of the covering part, DROP fsyncs only the covering part, fsync-off fsyncs nothing.
WITH dir_sync AS
(
    SELECT table, sum(ProfileEvents['DirectorySync']) AS synced
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND event_type = 'NewPart' AND part_name = 'all_1_1_1'
      AND table IN ('detach_fsync_on', 'detach_fsync_proj', 'detach_fsync_drop', 'detach_fsync_off')
    GROUP BY table
)
SELECT
    -- A DETACH fsyncs the clone leaf plus the ancestor chain past detached/ up to the disk root, so it
    -- fsyncs at least three more directories than a DROP (leaf + detached/ + the table dir it lives in).
    -- The +3 lower bound rejects a degraded walk that stops at detached/ and never persists detached/'s
    -- own entry in the table dir (the first-detach loss #111382 fixes). The real margin is far larger
    -- (leaf + detached/ + the full store/<prefix>/<uuid> chain + root); +3 stays robust to path depth.
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_on')
        >= (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_drop') + 3 AS clone_synced,
    -- A projection adds its <name>.proj subdir to the clone subtree, one more directory than the plain detach.
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_proj')
        > (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_on') AS proj_subtree_synced,
    -- With fsync_part_directory = 0 (the default) the clone fsync is gated off: no directory fsync.
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_off') = 0 AS off_not_synced,
    -- The covering NewPart rows must exist for the counts above to be meaningful.
    (SELECT count() FROM dir_sync) = 4 AS all_parts_logged;

-- The detached data must survive and re-attach with all rows (fsync on, on with a projection, off).
ALTER TABLE detach_fsync_on   ATTACH PARTITION tuple();
ALTER TABLE detach_fsync_proj ATTACH PARTITION tuple();
ALTER TABLE detach_fsync_off  ATTACH PARTITION tuple();

SELECT
    (SELECT count() FROM detach_fsync_on)   = 1000 AS on_preserved,
    (SELECT count() FROM detach_fsync_proj) = 1000 AS proj_preserved,
    (SELECT count() FROM detach_fsync_off)  = 1000 AS off_preserved;

DROP TABLE detach_fsync_on;
DROP TABLE detach_fsync_proj;
DROP TABLE detach_fsync_drop;
DROP TABLE detach_fsync_off;

-- Same durability contract for PACKED part storage. DataPartStorageOnDiskPacked::freeze is a fully
-- independent override of the Full-storage one; without the packed fix its DETACH clone hardlinks
-- data.packed into the detached/ directory and fsyncs nothing, so the clone directory entries roll
-- back on power loss exactly like the Full case. Forcing Packed storage (min_bytes_for_full_part_storage
-- high) and asserting the same relative DirectorySync signal guards that override.

DROP TABLE IF EXISTS detach_fsync_packed_on;
DROP TABLE IF EXISTS detach_fsync_packed_proj;
DROP TABLE IF EXISTS detach_fsync_packed_drop;
DROP TABLE IF EXISTS detach_fsync_packed_off;

CREATE TABLE detach_fsync_packed_on   (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
CREATE TABLE detach_fsync_packed_proj (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v)) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
CREATE TABLE detach_fsync_packed_drop (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 1, storage_policy = 'default', min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;
CREATE TABLE detach_fsync_packed_off  (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS fsync_part_directory = 0, storage_policy = 'default', min_bytes_for_full_part_storage = 1000000000, min_rows_for_full_part_storage = 1000000000;

INSERT INTO detach_fsync_packed_on   SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_packed_proj SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_packed_drop SELECT number, number % 10 FROM numbers(1000);
INSERT INTO detach_fsync_packed_off  SELECT number, number % 10 FROM numbers(1000);

-- The parts must actually be Packed for this group to test the packed freeze override.
SELECT
    (SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND active AND part_storage_type = 'Packed'
          AND table IN ('detach_fsync_packed_on', 'detach_fsync_packed_proj', 'detach_fsync_packed_drop', 'detach_fsync_packed_off')) = 4 AS all_packed;

ALTER TABLE detach_fsync_packed_on   DETACH PARTITION tuple();
ALTER TABLE detach_fsync_packed_proj DETACH PARTITION tuple();
ALTER TABLE detach_fsync_packed_drop DROP   PARTITION tuple();
ALTER TABLE detach_fsync_packed_off  DETACH PARTITION tuple();

SYSTEM FLUSH LOGS part_log;

WITH dir_sync AS
(
    SELECT table, sum(ProfileEvents['DirectorySync']) AS synced
    FROM system.part_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND database = currentDatabase() AND event_type = 'NewPart' AND part_name = 'all_1_1_1'
      AND table IN ('detach_fsync_packed_on', 'detach_fsync_packed_proj', 'detach_fsync_packed_drop', 'detach_fsync_packed_off')
    GROUP BY table
)
SELECT
    -- Same +3 lower bound as the Full group: the packed clone walk must reach past detached/ into the table dir.
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_packed_on')
        >= (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_packed_drop') + 3 AS clone_synced,
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_packed_proj')
        > (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_packed_on') AS proj_subtree_synced,
    (SELECT synced FROM dir_sync WHERE table = 'detach_fsync_packed_off') = 0 AS off_not_synced,
    (SELECT count() FROM dir_sync) = 4 AS all_parts_logged;

ALTER TABLE detach_fsync_packed_on   ATTACH PARTITION tuple();
ALTER TABLE detach_fsync_packed_proj ATTACH PARTITION tuple();
ALTER TABLE detach_fsync_packed_off  ATTACH PARTITION tuple();

SELECT
    (SELECT count() FROM detach_fsync_packed_on)   = 1000 AS on_preserved,
    (SELECT count() FROM detach_fsync_packed_proj) = 1000 AS proj_preserved,
    (SELECT count() FROM detach_fsync_packed_off)  = 1000 AS off_preserved;

DROP TABLE detach_fsync_packed_on;
DROP TABLE detach_fsync_packed_proj;
DROP TABLE detach_fsync_packed_drop;
DROP TABLE detach_fsync_packed_off;
