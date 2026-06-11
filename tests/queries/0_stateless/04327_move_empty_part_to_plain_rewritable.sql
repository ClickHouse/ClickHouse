-- Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree, no-parallel
-- Tag no-object-storage: relies on the local_disk / local_plain_rewritable disks from storage_conf_local.xml
-- Tag no-replicated-database, no-shared-merge-tree: plain rewritable is not shared between replicas
-- Tag no-parallel: SYSTEM DROP DISK METADATA CACHE affects server-wide disk metadata cache state

DROP TABLE IF EXISTS t_move_empty_pr;

-- Data lands on the local-metadata volume; the plain_rewritable volume is the move target.
CREATE TABLE t_move_empty_pr (c0 Int) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS remove_empty_parts = 0, storage_policy = 'local_to_plain_rewritable';

INSERT INTO t_move_empty_pr VALUES (1);

-- remove_empty_parts = 0 keeps the now-empty part after DROP PARTITION.
ALTER TABLE t_move_empty_pr DROP PARTITION tuple();

SELECT disk_name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_move_empty_pr' AND active;

-- The empty part has 0-byte column files and thus no blobs on the local disk. Moving it to a
-- plain_rewritable disk used to crash (front() on an empty StoredObjects vector). It must succeed.
ALTER TABLE t_move_empty_pr MOVE PARTITION tuple() TO DISK 'local_plain_rewritable';

SELECT disk_name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_move_empty_pr' AND active;

SELECT count() FROM t_move_empty_pr;

-- Reload plain_rewritable metadata from the object listing. plain_rewritable persists files by the
-- objects under the directory, so the empty part's 0-byte files survive a reload only if a real
-- 0-byte object was materialized (not just an in-memory metadata entry).
SYSTEM DROP DISK METADATA CACHE local_plain_rewritable;

SELECT disk_name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_move_empty_pr' AND active;

SELECT count() FROM t_move_empty_pr;

-- Moving the empty part back reads its (0-byte) objects, which must exist on the disk.
ALTER TABLE t_move_empty_pr MOVE PARTITION tuple() TO DISK 'local_disk';

SELECT disk_name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_move_empty_pr' AND active;

-- The part is still fully functional after the round trip.
SELECT count() FROM t_move_empty_pr;
INSERT INTO t_move_empty_pr VALUES (42);
SELECT * FROM t_move_empty_pr ORDER BY c0;

DROP TABLE t_move_empty_pr;
