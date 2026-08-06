#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# Regression test for restoring a backup that contains a table on a fully read-only storage policy
# (e.g. ClickHouse Cloud example datasets). The data of such a table lives on the shared read-only
# storage, so RESTORE must recreate the table and skip writing its parts (they are discovered from
# the disk itself). Before the fix the whole RESTORE failed with
# "Could not reserve ... because all disk volumes are readonly. (READONLY)".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="disks/04627/${CLICKHOUSE_DATABASE}/"
backup_name="04627_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

# Writer: a read-write plain_rewritable object-storage disk; populates the shared location.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (key UInt64) PARTITION BY key ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      name = 04627_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES (1), (2), (3)"

# Reader: the SAME storage, read-only — the shape of a shared/example dataset table.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (key UInt64) PARTITION BY key ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      read_only = true,
      name = 04627_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader"

# The backup of the read-only table contains both its metadata and its data files.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.reader TO Disk('backups', '${backup_name}')" | grep -o "BACKUP_CREATED"

# RESTORE must succeed: the table is recreated and the parts are discovered from the read-only disk.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.reader AS ${CLICKHOUSE_DATABASE}.reader_restored FROM Disk('backups', '${backup_name}')" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader_restored ORDER BY key"

# A partition-filtered RESTORE (a strict subset) onto read-only storage cannot honor the filter (the data
# phase is skipped, so the engine never applies it). It must fail closed instead of silently exposing the
# whole dataset.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.reader AS ${CLICKHOUSE_DATABASE}.reader_part PARTITIONS 1 FROM Disk('backups', '${backup_name}')" 2>&1 | grep -o "CANNOT_RESTORE_TABLE" | head -n1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader_part SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader_restored SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
