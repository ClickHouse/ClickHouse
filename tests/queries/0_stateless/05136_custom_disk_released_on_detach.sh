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
