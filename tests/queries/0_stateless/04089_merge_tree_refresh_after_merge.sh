#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree, no-fasttest
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# A readonly replica refreshing a shared plain_rewritable path must handle the writer having MERGED:
# the refresh adds the merged part and must also retire the parts that merge superseded.
#
# The retirement step is observable only through the part STATE. A refresh commits the merged part
# with the same Transaction::commit that demotes the parts it covers to Outdated, so the active set
# is {merged part} either way; the superseded parts are Deleting once grabbed for removal and stay
# Outdated otherwise. system.parts returns non-Active parts only when _state is selected, hence the
# grouping below.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

disk_path="disks/04089/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = 04089_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

# Same backing path, readonly and WITHOUT refresh_parts_interval: the background task would race the
# assertions below, so the refresh is driven explicitly by SYSTEM RESTART DISK instead.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      readonly = true,
      name = 04089_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

# The writer's own scheduler is otherwise free to merge these three parts before the first refresh.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES writer"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('aaa')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('bbb')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('ccc')"

${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK 04089_reader_${CLICKHOUSE_DATABASE}"
echo "reader before merge:"
${CLICKHOUSE_CLIENT} --query "
SELECT _state, count() FROM system.parts
WHERE database = currentDatabase() AND table = 'reader' GROUP BY _state ORDER BY _state"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES writer"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE writer FINAL"

${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK 04089_reader_${CLICKHOUSE_DATABASE}"
echo "reader after merge:"
${CLICKHOUSE_CLIENT} --query "
SELECT _state, count() FROM system.parts
WHERE database = currentDatabase() AND table = 'reader' GROUP BY _state ORDER BY _state"

echo "reader rows:"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM reader ORDER BY s"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
