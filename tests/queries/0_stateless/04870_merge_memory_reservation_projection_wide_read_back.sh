#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a rebuilt projection whose READ-BACK merge produces a Wide part out of Compact temporary parts. The rebuild
# squashes the projected rows into chunks (min_insert_block_size_rows / min_insert_block_size_bytes) and each
# temporary part is formatted from its own chunk's size - below min_bytes_for_wide_part they are all Compact -
# but MergeProjectionPartsTask then batches them into nested merges whose FutureMergedMutatedPart::assign
# re-runs choosePartFormat on the summed bytes and rows, so the final projection part comes out Wide once the
# whole rebuilt volume clears the wide-part thresholds. The estimate prices the read-back writer by that final
# format rather than by the temporary parts', so under a pathologically small soft limit the (unconditionally
# reserved) OPTIMIZE must still run to a single part with a Wide rebuilt projection, and must not error while
# estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The squash thresholds are query-level settings the merge picks up from its background context; setting them
# this small makes the rebuild flush ~1 MB Compact chunks while the whole projection volume (~14 MB) stays
# well above the default min_bytes_for_wide_part (10 MiB), so the read-back merges the Compact temporary
# parts into a Wide projection part.
${CLICKHOUSE_LOCAL} --min_insert_block_size_rows=2000 --min_insert_block_size_bytes=1000000 -q "
    CREATE TABLE t_merge_mem_wide_read_back
    (
        k UInt64,
        v String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_wide_read_back;
    INSERT INTO t_merge_mem_wide_read_back SELECT number, repeat('a', 600) FROM numbers(8000);
    INSERT INTO t_merge_mem_wide_read_back SELECT number, repeat('b', 600) FROM numbers(8000, 8000);
    INSERT INTO t_merge_mem_wide_read_back SELECT number, repeat('c', 600) FROM numbers(16000, 8000);

    ALTER TABLE t_merge_mem_wide_read_back ADD PROJECTION p_wide (SELECT k, v ORDER BY v);
    SYSTEM START MERGES t_merge_mem_wide_read_back;

    OPTIMIZE TABLE t_merge_mem_wide_read_back FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_wide_read_back;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_wide_read_back' AND active;
    SELECT name, part_type, rows FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_wide_read_back' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
