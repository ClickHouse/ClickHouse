#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# A writer table (adaptive granularity) creates a Compact part on a plain_rewritable path. A reader
# table with non-adaptive granularity (index_granularity_bytes = 0, so canUsePolymorphicParts() is
# false) shares the same backing path. Previously the part-load path rejected the Compact part with a
# LOGICAL_ERROR ("... table does not support polymorphic parts"), aborting the server in
# debug/sanitizer builds. An existing Compact part is always adaptive, so it must load regardless of
# the reader's current policy. Both callers that reach the load path are covered: SYSTEM RESTART DISK
# and the background `refresh_parts_interval` task.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader_refresh SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = poly_load_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04371/${CLICKHOUSE_DATABASE}/')
"

# Same backing path, readonly, and non-adaptive granularity (index_granularity_bytes = 0) so that
# canUsePolymorphicParts() is false for this table. Keep min_*_for_wide_part = 0: on a non-adaptive
# table a nonzero threshold logs a "settings will be ignored" warning at CREATE that trips the empty-stderr check.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (s String) ORDER BY ()
SETTINGS table_disk = true, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
  disk = disk(
      readonly = true,
      name = poly_load_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04371/${CLICKHOUSE_DATABASE}/')
"

# Same shared path and settings, but this reader picks parts up via the background
# `refresh_parts_interval` task. Created before the insert on purpose: the part must arrive after the
# initial load, so the refresh task rather than startup is what loads it. The disk must stay readonly,
# otherwise the task is not started at all.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_refresh (s String) ORDER BY ()
SETTINGS table_disk = true, refresh_parts_interval = 1,
         index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
  disk = disk(
      readonly = true,
      name = poly_load_refresh_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/04371/${CLICKHOUSE_DATABASE}/')
"

# A single small insert produces a Compact part.
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"
echo "writer part type:"
${CLICKHOUSE_CLIENT} --query "SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'writer' AND active"

# Reloads the readonly reader's part list; it now loads the writer's Compact part.
${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK poly_load_reader_${CLICKHOUSE_DATABASE}"
echo "reader after restart:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(s) FROM reader"

# Wait for the background refresh to observe the part written above.
for _ in {1..300}; do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader_refresh")" = "1" ] && break
    sleep 0.1
done
echo "reader after background refresh:"
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(s) FROM reader_refresh"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader_refresh SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
