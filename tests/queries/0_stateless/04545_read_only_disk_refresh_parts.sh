#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-random-settings: enable after root causing flakiness
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# A read-only object-storage disk created via `read_only = true` is wrapped in ReadOnlyDiskWrapper. That wrapper must:
#   (1) forward refresh() so that `refresh_parts_interval` picks up parts written by another server;
#   (2) not be mistaken for a non-object-storage disk, so `table_disk = true` is accepted.
# Before the fix, creating the reader table failed with "table_disk is not supported for non-ObjectStorage disks",
# and even when created it never observed new parts (logs kept saying "added 0 items").

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

disk_path="disks/04545/${CLICKHOUSE_DATABASE}/"

# Writer: a read-write plain_rewritable object-storage disk.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = 04545_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

# Reader: the SAME storage, but read-only (wrapped in ReadOnlyDiskWrapper via `read_only = true`).
# This CREATE fails before the fix (issue 2: table_disk validation does not look through the wrapper).
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (s String) ORDER BY ()
SETTINGS table_disk = true, refresh_parts_interval = 1,
  disk = disk(
      read_only = true,
      name = 04545_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"

# The read-only reader must observe the newly written part via refresh_parts_interval, without a restart.
# Before the fix (issue 1: refresh() is a no-op on the wrapper) 'Hello' would never appear here.
for _ in {1..300}; do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader")" = "Hello" ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader"

# Negative: a disk with a LOCAL metadata layer must stay rejected for `table_disk` (its metadata is not
# self-contained on the object storage). Assert the metadata-specific message so the check actually covers the
# metadata-kind predicate: the generic "is not supported for" also matches the older cast-based gate.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_local (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      read_only = true,
      name = 04545_local_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = local,
      path = 'disks/04545_local/${CLICKHOUSE_DATABASE}/')
" 2>&1 | grep -qF "requires metadata stored on the object storage" && echo "local metadata read-only disk rejected for table_disk"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader_local SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
