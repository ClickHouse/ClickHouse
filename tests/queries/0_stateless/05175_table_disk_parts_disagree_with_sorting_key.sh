#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: creates custom object-storage disks
# Tag no-replicated-database: every replica would run the CREATE and mount the same disk root
# Tag no-shared-merge-tree: the parts found under the root are not the part set SharedMergeTree reads

# `table_disk` mounts the parts already present at the disk root, and nothing records the sorting key
# a part was written with. A second table over the same root declaring the opposite direction therefore
# reads parts that do not satisfy its own sorting key: the parts splitter used to abort on a chassert in
# debug builds and to throw LOGICAL_ERROR in release builds. It must report INCORRECT_DATA instead.
#
# index_granularity_bytes is pinned on the writer so the parts keep adaptive granularity: the last mark
# of such a part carries a primary key value, which is what makes the mismatch visible to the splitter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_path="disks/05175/${CLICKHOUSE_DATABASE}/"

# The writer owns the root and writes 4 unmerged parts, all in ascending key order.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (key Int32, val UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true, index_granularity_bytes = 10485760,
  disk = disk(
      name = 05175_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES writer"
for offset in 0 50 100 150; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SELECT number + ${offset}, number FROM numbers(50)"
done

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(key) FROM writer"
# 4 parts, and every part has more than one mark, i.e. its last mark carries an index value.
${CLICKHOUSE_CLIENT} --query "
SELECT count(), min(marks) > 1 FROM system.parts
WHERE database = currentDatabase() AND table = 'writer' AND active"

# Same root, opposite declared direction.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_desc (key Int32, val UInt32) ENGINE = MergeTree ORDER BY key DESC
SETTINGS table_disk = true,
  disk = disk(
      name = 05175_reader_desc_${CLICKHOUSE_DATABASE},
      read_only = true,
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

# Same root, declared in the order the parts were written. This reads through the same splitter.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_asc (key Int32, val UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      name = 05175_reader_asc_${CLICKHOUSE_DATABASE},
      read_only = true,
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

# The mirror of the pair above, on its own root: parts written descending, mounted by a table declaring
# ascending. The two orders are compared by different branches, and the reader above reaches only one.
disk_path_desc="disks/05175_desc/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer_desc (key Int32, val UInt32) ENGINE = MergeTree ORDER BY key DESC
SETTINGS table_disk = true, index_granularity_bytes = 10485760,
  disk = disk(
      name = 05175_writer_desc_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path_desc}')
"

${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES writer_desc"
for offset in 0 50 100 150; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO writer_desc SELECT number + ${offset}, number FROM numbers(50)"
done

${CLICKHOUSE_CLIENT} --query "
SELECT count(), min(marks) > 1 FROM system.parts
WHERE database = currentDatabase() AND table = 'writer_desc' AND active"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader_asc_mismatch (key Int32, val UInt32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      name = 05175_reader_asc_mismatch_${CLICKHOUSE_DATABASE},
      read_only = true,
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path_desc}')
"

# The injected split is not applied to a parallel-replicas read, so the read has to be a plain local
# one for the ranges to reach the splitter at all.
split_read()
{
    ${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(key) FROM $1 SETTINGS
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1,
        max_threads = 8, use_query_condition_cache = 0, enable_parallel_replicas = 0"
}

# The error names the offending part, so assert that and not only the code.
error=$(split_read reader_desc 2>&1)
echo "$error" | grep -oE "INCORRECT_DATA" | head -1
echo "$error" | grep -oE "Part all_[0-9_]+ is not sorted by the sorting key declared by this table" | head -1

error=$(split_read reader_asc_mismatch 2>&1)
echo "$error" | grep -oE "INCORRECT_DATA" | head -1
echo "$error" | grep -oE "Part all_[0-9_]+ is not sorted by the sorting key declared by this table" | head -1

# Control: without it the lines above would pass for an empty root or a broken disk path too.
split_read reader_asc

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader_desc"
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader_asc"
${CLICKHOUSE_CLIENT} --query "DROP TABLE reader_asc_mismatch"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer_desc"
