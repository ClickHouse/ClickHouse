#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# Two tables share a single plain_rewritable backing path: `writer` on a read-write disk,
# `reader` on a readonly disk. The reader caches both the disk namespace and its part list
# in memory, so it does not observe the writer's new part on its own. A single
# `SYSTEM RESTART DISK` reloads the disk namespace and re-scans the part list of readonly
# tables on that disk, so the reader observes the row without DETACH/ATTACH or a restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = restart_disk_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04318/${CLICKHOUSE_DATABASE}/')
"

# Same backing path, but readonly and WITHOUT refresh_parts_interval, so the only way for
# this replica to notice external writes is an explicit SYSTEM RESTART DISK.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      readonly = true,
      name = restart_disk_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04318/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"

# The reader has not reloaded its disk yet, so it does not see the new row.
echo "before restart:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader"

# One command reloads the disk namespace and re-scans the readonly table's parts.
${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK restart_disk_reader_${CLICKHOUSE_DATABASE}"
echo "after restart:"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
