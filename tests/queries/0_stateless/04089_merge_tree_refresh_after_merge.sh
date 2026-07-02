#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree, no-fasttest
# Test: Verify that reader with refresh_parts_interval correctly handles merges done by writer
# Covers: MergeTreeData::refreshDataParts at MergeTreeData.cpp:2620 - merge handling in readonly tables

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer_04089 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader_04089 SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer_04089 (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = 04089_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04089/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_04089 (s String) ORDER BY ()
SETTINGS table_disk = true, refresh_parts_interval = 1,
  disk = disk(
      readonly = true,
      name = 04089_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04089/${CLICKHOUSE_DATABASE}/')
"

# Insert 3 parts
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer_04089 VALUES ('aaa')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer_04089 VALUES ('bbb')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer_04089 VALUES ('ccc')"

# Wait for reader to see all 3 rows
for i in $(seq 1 30); do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader_04089")
    if [ "$result" -eq 3 ]; then
        break;
    fi
    sleep 0.5;
done

# Merge all parts in writer
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE writer_04089 FINAL"

# Wait for reader to refresh and verify no duplicate rows
sleep 3

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader_04089"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader_04089 ORDER BY s"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader_04089 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer_04089 SYNC"
