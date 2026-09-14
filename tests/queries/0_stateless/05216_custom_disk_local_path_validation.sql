-- Tags: no-fasttest
-- Tag no-fasttest: custom disks are not configured in fasttest

DROP TABLE IF EXISTS t_05216;

-- The name of a disk is used verbatim as a path component of the state the server keeps for that disk,
-- so it must be a plain name and not a path.
CREATE TABLE t_05216 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(name = '../user_scripts/05216', type = object_storage, object_storage_type = local, path = '/var/lib/clickhouse/disks/05216_name/'); -- { serverError BAD_ARGUMENTS }

-- The directory of the data has to be inside `custom_local_disks_base_directory` ...
CREATE TABLE t_05216 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(name = '05216_outside_data', type = object_storage, object_storage_type = local, path = '/etc/clickhouse-server/05216/'); -- { serverError BAD_ARGUMENTS }

-- ... and a path that leaves it again is outside it as well.
CREATE TABLE t_05216 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(name = '05216_outside_traversal', type = object_storage, object_storage_type = local, path = '/var/lib/clickhouse/disks/../user_files/05216/'); -- { serverError BAD_ARGUMENTS }

-- The same holds for the directory the metadata of the disk is written to.
CREATE TABLE t_05216 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(name = '05216_outside_metadata', type = object_storage, object_storage_type = local, path = '/var/lib/clickhouse/disks/05216_metadata/', metadata_path = '/var/lib/clickhouse/disks/../user_files/05216_metadata/'); -- { serverError BAD_ARGUMENTS }

-- A disk rejected by those checks must not stay usable by the statements that follow. Named one by
-- one rather than by a pattern: the disks that the statements below create on purpose stay
-- registered for the lifetime of the server, including for a second run of this test.
SELECT count() FROM system.disks
WHERE name IN ('../user_scripts/05216', '05216_outside_data', '05216_outside_traversal', '05216_outside_metadata');

-- A disk inside the base directory keeps working.
CREATE TABLE t_05216 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(name = '05216_ok', type = object_storage, object_storage_type = local, path = '/var/lib/clickhouse/disks/05216_ok/');
INSERT INTO t_05216 VALUES (1);
SELECT * FROM t_05216;
SELECT count() FROM system.disks WHERE name = '05216_ok';

DROP TABLE t_05216;

-- A disk that refers to a disk of the server configuration keeps working: the location it uses is
-- the one the administrator configured, not one this query named.
CREATE TABLE t_05216_cache (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = cache, name = '05216_cache', path = '05216_cache/', max_size = '1Mi', disk = 'local_disk');
INSERT INTO t_05216_cache VALUES (2);
SELECT * FROM t_05216_cache;

DROP TABLE t_05216_cache;
