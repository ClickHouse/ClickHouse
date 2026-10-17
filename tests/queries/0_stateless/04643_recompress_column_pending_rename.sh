#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# The test uses `SYSTEM STOP MERGES` to keep two mutations of a `ReplicatedMergeTree` table pending
# at the same time so that they can be combined into a single mutate task, and polls
# `system.mutations` of the single local replica -- both are specific to a plain replicated table
# on the local server, so shared MergeTree and the Replicated database (where the ALTERs go
# through the DDL queue) are excluded. Whether the two mutations really end up in one mutate task
# depends on when the merge-selecting task runs, so the column must come out recompressed either
# way; the single-`ALTER` case at the end of the test covers the combined task deterministically.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ALTER TABLE ... RECOMPRESS COLUMN` of a column that reaches the mutation through a rename:
# when the mutation materializing `RENAME COLUMN a TO b` and the `RECOMPRESS COLUMN b` mutation
# are combined into one mutate task, the source part still stores the streams under the old
# name `a`, so the in-place wide-part fast path cannot resolve them under `b`. Such a column
# must be routed to the whole-part rewrite; the bug was that it was silently skipped instead:
# the mutation finished successfully with the column renamed but still compressed with its old
# codec.

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_recompress_pending_rename"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_recompress_pending_rename (id UInt64, a String CODEC(NONE))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', 'r1')
    ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             min_level_for_full_part_storage = 0"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_recompress_pending_rename SELECT number, repeat('a', 200) FROM numbers(10000)"

# The codec change is applied before merges are stopped, so that the recompression mutation cannot
# run while the codec change is still queued: it is a metadata-only change, and a metadata change
# is applied only after the previous one (here the rename) has been fully finished, including its
# data mutation -- which stopped merges hold back. A recompression mutation executing before the
# replica applies the codec change would recompress the column with its previous codec, i.e. do
# nothing. The rename below keeps the data of the column as it is, so it stays uncompressed.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_recompress_pending_rename MODIFY COLUMN a String CODEC(ZSTD(3))"

# Stopping merges also stops mutations, so the rename-materializing mutation and the
# recompression mutation stay queued together and can be combined into one mutate task.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_recompress_pending_rename"

${CLICKHOUSE_CLIENT} --alter_sync 0 --query "ALTER TABLE t_recompress_pending_rename RENAME COLUMN a TO b"

# The metadata change of the rename is applied asynchronously with `alter_sync = 0`.
for _ in {1..300}
do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't_recompress_pending_rename' AND name = 'b'")" = "1" ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --alter_sync 0 --query "ALTER TABLE t_recompress_pending_rename RECOMPRESS COLUMN b"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_recompress_pending_rename"

# Both mutations (the rename and the recompression) must be present and finished; a plain
# `countIf(is_done) = count()` would also be true for an empty `system.mutations`, which the
# replication queue populates asynchronously.
for _ in {1..600}
do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() = 2 AND countIf(is_done) = 2 FROM system.mutations WHERE database = currentDatabase() AND table = 't_recompress_pending_rename'")" = "1" ] && break
    sleep 0.5
done

# 10000 rows of repeat('a', 200) are ~2 MB under `CODEC(NONE)` and ~50 KB under `ZSTD(3)`.
echo "recompressed under new name: $(${CLICKHOUSE_CLIENT} --query "SELECT sum(data_compressed_bytes) < 500000 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_recompress_pending_rename' AND column = 'b' AND active")"
${CLICKHOUSE_CLIENT} --query "SELECT 'data intact', count() FROM t_recompress_pending_rename WHERE b = repeat('a', 200)"
${CLICKHOUSE_CLIENT} --check_query_single_value_result 1 --query "CHECK TABLE t_recompress_pending_rename"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_recompress_pending_rename"

# The same combination expressed in a single ALTER statement: `RENAME COLUMN` and
# `RECOMPRESS COLUMN` of the renamed column in one query.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_recompress_rename_combined"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_recompress_rename_combined (id UInt64, x String CODEC(NONE))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', 'r1')
    ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             min_level_for_full_part_storage = 0"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_recompress_rename_combined SELECT number, repeat('a', 200) FROM numbers(10000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_recompress_rename_combined MODIFY COLUMN x String CODEC(ZSTD(3))"
${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "ALTER TABLE t_recompress_rename_combined RENAME COLUMN x TO y, RECOMPRESS COLUMN y"

echo "combined ALTER recompressed: $(${CLICKHOUSE_CLIENT} --query "SELECT sum(data_compressed_bytes) < 500000 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_recompress_rename_combined' AND column = 'y' AND active")"
${CLICKHOUSE_CLIENT} --query "SELECT 'data intact', count() FROM t_recompress_rename_combined WHERE y = repeat('a', 200)"
${CLICKHOUSE_CLIENT} --check_query_single_value_result 1 --query "CHECK TABLE t_recompress_rename_combined"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_recompress_rename_combined"
