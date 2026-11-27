#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: SYSTEM ENABLE FAILPOINT toggles server-global state.
# Tags no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree: mirror
#   03362_merge_tree_with_background_refresh, the plain_rewritable read-only refresh test this is based on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

# A "writer" table on a plain_rewritable disk and a read-only "reader" table over the same path.
# The reader has all-read-only disks and a non-zero refresh_parts_interval, so it runs the background
# task MergeTreeData::refreshDataParts that periodically rescans the disk for new parts.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = 04421_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04421/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (s String) ORDER BY ()
SETTINGS table_disk = true, refresh_parts_interval = 1,
  disk = disk(
      readonly = true,
      name = 04421_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04421/${CLICKHOUSE_DATABASE}/')
"

# Sanity check: the background refresh picks up the first part.
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"
while true
do
    [[ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader WHERE s = 'Hello'")" == "1" ]] && break
    sleep 0.1
done

# Inject a one-time transient error into the next background refresh. Before the fix the refresh task
# only rescheduled itself on the success path, so a single exception stopped it permanently and the
# read-only table stayed stale until the server restarted.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_tree_refresh_parts_throw_once"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('World')"

# The refresh task must recover from the transient error and eventually pick up the new part. Poll
# with a bounded timeout: on the buggy version the task is dead, so the part never appears, the loop
# exhausts, and the final SELECT prints 0 instead of 1, failing the reference comparison.
for _ in {1..120}
do
    [[ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader WHERE s = 'World'")" == "1" ]] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader WHERE s = 'World'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
