#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-object-storage: the test defines its own disks
# Tag no-replicated-database: plain rewritable should not be shared between replicas, and hypothetical indexes are session-scoped
# Tag no-shared-merge-tree: does not support replication

# A disk created with `read_only = true` is wrapped in `ReadOnlyDiskWrapper`, which has to report the
# capabilities of the disk it wraps. `supportsHardLinks` decides whether the operations that need hard
# links are offered at all, so a read-only attachment of a `plain_rewritable` disk without
# `enable_hard_links` must refuse them for the same reason as the disk itself. `ALTER` and mutations are
# rejected earlier on a read-only table, so the check is reachable through `CREATE HYPOTHETICAL INDEX`,
# which mirrors it because an index can only be added to a table whose parts can be hard-linked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="disks/05234/${CLICKHOUSE_DATABASE}/"
disk_path_with_hard_links="disks/05234_hard_links/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS writer SYNC;
DROP TABLE IF EXISTS reader SYNC;

CREATE TABLE writer (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(
    name = '05234_writer_${CLICKHOUSE_DATABASE}',
    type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
    path = '${disk_path}');

INSERT INTO writer VALUES (1, 'Hello');

CREATE TABLE reader (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(
    read_only = true,
    name = '05234_reader_${CLICKHOUSE_DATABASE}',
    type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
    path = '${disk_path}');
"

echo '-- the read-only reader sees the data'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader"

echo '-- without enable_hard_links the read-only attachment reports the capability of the disk it wraps'
${CLICKHOUSE_CLIENT} --query "CREATE HYPOTHETICAL INDEX h ON reader value TYPE minmax GRANULARITY 1" 2>&1 |
    grep -o -m1 'Hypothetical indexes are not supported on immutable disk'

echo '-- the writer of the same disk answers the same way'
${CLICKHOUSE_CLIENT} --query "CREATE HYPOTHETICAL INDEX h ON writer value TYPE minmax GRANULARITY 1" 2>&1 |
    grep -o -m1 'Hypothetical indexes are not supported on immutable disk'

# The wrapper must not answer `false` unconditionally either: with the setting on, the read-only
# attachment accepts what the disk it wraps accepts.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS writer_hard_links SYNC;
DROP TABLE IF EXISTS reader_hard_links SYNC;

CREATE TABLE writer_hard_links (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(
    name = '05234_writer_hl_${CLICKHOUSE_DATABASE}',
    type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
    enable_hard_links = 1,
    path = '${disk_path_with_hard_links}');

INSERT INTO writer_hard_links VALUES (1, 'Hello');

CREATE TABLE reader_hard_links (key Int32, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(
    read_only = true,
    name = '05234_reader_hl_${CLICKHOUSE_DATABASE}',
    type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
    enable_hard_links = 1,
    path = '${disk_path_with_hard_links}');
"

echo '-- with enable_hard_links the read-only attachment accepts it'
# Hypothetical indexes are session-scoped, so the definition and the readback share one invocation.
${CLICKHOUSE_CLIENT} -m --query "
CREATE HYPOTHETICAL INDEX h ON reader_hard_links value TYPE minmax GRANULARITY 1;
SELECT name, table FROM system.hypothetical_indexes ORDER BY table;
"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE reader SYNC;
DROP TABLE writer SYNC;
DROP TABLE reader_hard_links SYNC;
DROP TABLE writer_hard_links SYNC;
"
