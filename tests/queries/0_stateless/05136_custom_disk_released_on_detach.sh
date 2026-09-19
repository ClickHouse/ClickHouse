#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A disk defined inline with `disk(...)` exists only for the tables and databases that define it,
# so it has to be unregistered once the last of them is dropped or detached.

table_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_table_disk"
database_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_database_disk"
database="${CLICKHOUSE_TEST_UNIQUE_NAME}_database"

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

$CLIENT -q "
DROP TABLE IF EXISTS test_custom_disk SYNC;

CREATE TABLE test_custom_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$table_disk', type = 'local_blob_storage', path = '$table_disk/');

INSERT INTO test_custom_disk SELECT number FROM numbers(3);
SELECT 'after create', count() FROM system.disks WHERE name = '$table_disk';

DETACH TABLE test_custom_disk;
SELECT 'after detach', count() FROM system.disks WHERE name = '$table_disk';

ATTACH TABLE test_custom_disk;
SELECT 'after attach', count() FROM system.disks WHERE name = '$table_disk';
SELECT 'rows after attach', count() FROM test_custom_disk;

DROP TABLE test_custom_disk SYNC;
SELECT 'after drop', count() FROM system.disks WHERE name = '$table_disk';
"

# The same for a database that keeps its metadata on such a disk, together with a table of its own.
$CLIENT -q "
DROP DATABASE IF EXISTS $database SYNC;

CREATE DATABASE $database ENGINE = Atomic
SETTINGS disk = disk(name = '$database_disk', type = local, path = '${CLICKHOUSE_DISKS_FILES}/$database_disk/');

CREATE TABLE $database.test_custom_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$table_disk', type = 'local_blob_storage', path = '$table_disk/');

INSERT INTO $database.test_custom_disk SELECT number FROM numbers(4);
SELECT 'database disks', count() FROM system.disks WHERE name IN ('$table_disk', '$database_disk');

DETACH DATABASE $database;
"

$CLIENT -q "SELECT 'after detach database', count() FROM system.disks WHERE name IN ('$table_disk', '$database_disk')"

$CLIENT -q "
ATTACH DATABASE $database;
SELECT 'rows after attach database', count() FROM $database.test_custom_disk;
SELECT 'after attach database', count() FROM system.disks WHERE name IN ('$table_disk', '$database_disk');

DROP DATABASE $database SYNC;
"

$CLIENT -q "SELECT 'after drop database', count() FROM system.disks WHERE name IN ('$table_disk', '$database_disk')"

# A definition may wrap another one, e.g. `disk(type = cache, disk = disk(...))`. The wrapper keeps
# a reference to the disk it wraps, so the disks have to be released from the outside in: the inner
# disk is unregistered and shut down after the wrapper, not left behind.
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"
encrypted_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_encrypted_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_cache_disk SYNC;
DROP TABLE IF EXISTS test_encrypted_disk SYNC;

CREATE TABLE test_cache_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', path = '$cache_disk/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '$inner_disk/'));

-- The same inner definition, shared by both wrappers.
CREATE TABLE test_encrypted_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$encrypted_disk', type = encrypted, key = '1234567812345678',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '$inner_disk/'));

INSERT INTO test_cache_disk SELECT number FROM numbers(5);
INSERT INTO test_encrypted_disk SELECT number FROM numbers(6);
SELECT 'nested disks after create', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

DROP TABLE test_cache_disk SYNC;
SELECT 'nested disks after dropping the cache wrapper', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

DETACH TABLE test_encrypted_disk;
SELECT 'nested disks after detaching the encrypted wrapper', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

ATTACH TABLE test_encrypted_disk;
SELECT 'rows after attaching the encrypted wrapper', count() FROM test_encrypted_disk;
SELECT 'nested disks after attach', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

DROP TABLE test_encrypted_disk SYNC;
SELECT 'nested disks after drop', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

-- The object storage disks were shut down when their last table was gone, the inner one only after
-- both wrappers. The encrypted wrapper has nothing to shut down, so it logs nothing.
SYSTEM FLUSH LOGS text_log;
SELECT 'shut down', replaceAll(replaceAll(message, '$inner_disk', 'inner'), '$cache_disk', 'cache')
FROM system.text_log
WHERE event_date >= yesterday() AND message LIKE 'Disk % shut down'
    AND (message LIKE '%$inner_disk%' OR message LIKE '%$cache_disk%' OR message LIKE '%$encrypted_disk%')
ORDER BY event_time_microseconds;
"
