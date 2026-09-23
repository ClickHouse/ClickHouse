#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-fasttest: the `encrypted` disk type is not available in the fast test build
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

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
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));

-- The same inner definition, shared by both wrappers.
CREATE TABLE test_encrypted_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$encrypted_disk', type = encrypted, key = '1234567812345678',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));

INSERT INTO test_cache_disk SELECT number FROM numbers(5);
INSERT INTO test_encrypted_disk SELECT number FROM numbers(6);
SELECT 'nested disks after create', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

DROP TABLE test_cache_disk SYNC;
SELECT 'nested disks after dropping the cache wrapper', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

-- Dropping the cache wrapper must leave the inner disk it shares with the other wrapper working.
INSERT INTO test_encrypted_disk SELECT number FROM numbers(7);
SELECT 'rows after dropping the cache wrapper', count() FROM test_encrypted_disk;

DETACH TABLE test_encrypted_disk;
SELECT 'nested disks after detaching the encrypted wrapper', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

ATTACH TABLE test_encrypted_disk;
SELECT 'rows after attaching the encrypted wrapper', count() FROM test_encrypted_disk;
SELECT 'nested disks after attach', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');

DROP TABLE test_encrypted_disk SYNC;
SELECT 'nested disks after drop', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk', '$encrypted_disk');
"
