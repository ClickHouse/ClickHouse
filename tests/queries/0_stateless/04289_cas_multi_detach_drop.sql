-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- B46/B47: multiple partitions detached on a cas disk must COEXIST under the one
-- shared "detached" ref, and DROP DETACHED PARTITION ALL must remove them.
--   B46: each DETACH PARTITION clones one part into detached/<detached_part>/ via a fresh CA commit;
--        the commit used to REWRITE the shared "detached" ref, so each detach overwrote the previous
--        one and only the last detached part was listed. commit now MERGES into the existing detached
--        ref's manifest + sidecar, so all detached parts coexist.
--   B47: DROP DETACHED PARTITION first renames the detached part to "deleting_<part>"
--        (PartsTemporaryRename) then removes it; CA moveDirectory ignored a detached->detached rename
--        (the rename was a no-op, so removeRecursive on the renamed dir found nothing). moveDirectory
--        now re-keys the detached part dir within the shared detached ref, and removeRecursive handles a
--        detached part directory by removing only that part's keys from the shared ref.

DROP TABLE IF EXISTS t_cas_multi_detach;

CREATE TABLE t_cas_multi_detach (p UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY v
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04289',
    name = '04289_cas_multi_detach',
    path = '04289_cas_multi_detach_pool/');

INSERT INTO t_cas_multi_detach VALUES (1, 1), (2, 2), (3, 3);
SELECT 'active_parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_cas_multi_detach' AND active;

ALTER TABLE t_cas_multi_detach DETACH PARTITION ALL;

-- All three partitions must be listed as detached parts (not just the last one detached).
SELECT 'detached_after', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cas_multi_detach';
SELECT 'detached_names', name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cas_multi_detach' ORDER BY name;

ALTER TABLE t_cas_multi_detach DROP DETACHED PARTITION ALL SETTINGS allow_drop_detached = 1;

-- DROP DETACHED PARTITION ALL must remove every detached part.
SELECT 'detached_after_drop', count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cas_multi_detach';

DROP TABLE t_cas_multi_detach;
SELECT 'dropped_ok';
