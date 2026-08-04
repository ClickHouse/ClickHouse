-- Tags: no-fasttest
-- ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

-- B44: reading system.detached_parts.modification_time of a part detached on a cas
-- disk must NOT throw. The detached part is stored under a ref named "detached" whose manifest keys
-- are shaped <detached_part>/<file>; system.detached_parts reads the modification time by calling
-- IDisk::getLastModified on the detached part DIRECTORY (<table>/detached/<detached_part>). Before the
-- fix, parsePartFilePath reported part_name="detached" + a non-empty file equal to the detached part
-- directory name, so getLastModified fell through to the part-file manifest lookup and threw
-- "ContentAddressed: file <detached_part> not in manifest". getLastModified now recognises the detached
-- part directory and reports the "detached" ref manifest object's mtime.

DROP TABLE IF EXISTS t_cas_detach_mtime;

CREATE TABLE t_cas_detach_mtime (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04288',
    name = '04288_cas_detach_mtime',
    path = '04288_cas_detach_mtime_pool/');

INSERT INTO t_cas_detach_mtime SELECT number, number * 2 FROM numbers(50);

SELECT 'count_before', count() FROM t_cas_detach_mtime;

ALTER TABLE t_cas_detach_mtime DETACH PARTITION tuple();

SELECT 'count_after', count() FROM t_cas_detach_mtime;

-- The modification_time read must succeed (be non-NULL) instead of throwing FILE_DOESNT_EXIST.
SELECT 'detached_mtime_readable', name, modification_time IS NOT NULL
FROM system.detached_parts
WHERE database = currentDatabase() AND table = 't_cas_detach_mtime'
ORDER BY name;

DROP TABLE t_cas_detach_mtime;
SELECT 'dropped_ok';
