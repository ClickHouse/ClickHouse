#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage
#
# `no-fasttest`: reading the part directory off local disk is not reliably available on the Fast
# test macOS runner. `no-object-storage` / `no-shared-merge-tree` / `no-replicated-database`: the
# test lists the files of a real local part directory of a single, deterministic active part.
#
# The part-level minmax index is materialized as a PREFIX of its column list - the partition key
# columns, then `_block_number`, then `_block_offset` - never as an arbitrary subset. Nothing that
# reads the index can describe a part that materialized column `k + 1` but not column `k`: the
# width is what carries "how much of the index this part has" (`MinMaxIndex::merge` truncates to
# the shorter of two indices, `getProbablyWrittenFiles` derives the file names from the leading
# columns, `KeyCondition::checkInHyperrectangle` treats everything past the width as unknown).
# See the note on `MergeTreePartMinMaxIndexColumns` in `Core/SettingsEnums.h`.
#
# The paths that produce a part with a partly materialized block column segment are the mutations
# that do not rewrite the whole part: they only hardlink the source part's files and then write out
# whatever of the inherited index they can reconstruct. This walks every shape of that and checks
# the files left on disk are a prefix each time - in particular that an unrepairable `_block_offset`
# truncates the index instead of punching a hole in it.
#
# Every table pins `min_bytes_for_full_part_storage = 0`: with packed storage the part is a single
# `data.packed` file with no per-file layout to inspect, and the mutation takes the whole-part
# rewrite path instead of hardlinking, so neither the files nor the code path under test exist.
# `min_bytes_for_wide_part = 0` keeps the part Wide for the same reason, and
# `replace_long_file_name_to_hash = 0` keeps the file names literal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Prints the minmax columns the single active part has a file for, in index order, and whether
# that set is a prefix of the index's column list.
check_prefix()
{
    local scenario=$1
    local table=$2

    local part_dir
    part_dir=$(${CLICKHOUSE_CLIENT} -q "
        SELECT path FROM system.parts
        WHERE database = currentDatabase() AND table = '$table' AND active")

    local materialized=()
    local bits=""
    for column in p _block_number _block_offset
    do
        if [ -f "${part_dir}/minmax_${column}.idx" ]
        then
            materialized+=("$column")
            bits+="1"
        else
            bits+="0"
        fi
    done

    local prefix=0
    [[ $bits =~ ^1*0*$ ]] && prefix=1

    local joined
    joined=$(IFS=,; echo "${materialized[*]}")
    echo -e "$scenario\t${joined:--}\tprefix=$prefix"
}

# Fully materialized: a mutation of a never-mutated single-block part knows both block column
# ranges - the number of its one block, and the offsets of every row of that block.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_minmax_prefix_fresh SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_minmax_prefix_fresh (p UInt8, s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'with_block_number_offset',
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_fresh SELECT 1, toString(number) FROM numbers(9)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_fresh UPDATE s = 'x' WHERE 1 SETTINGS mutations_sync = 2"
check_prefix fresh_mutation t_minmax_prefix_fresh
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_minmax_prefix_fresh SYNC"

# The setting was widened on a live table, so the part in memory carries an index without the block
# column slots at all. Both are reconstructed, and the index is fully materialized again.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_minmax_prefix_widened SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_minmax_prefix_widened (p UInt8, s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only',
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_widened SELECT 1, toString(number) FROM numbers(9)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_widened MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset'"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_widened UPDATE s = 'x' WHERE 1 SETTINGS mutations_sync = 2"
check_prefix widened_setting t_minmax_prefix_widened
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_minmax_prefix_widened SYNC"

# The part has already been through a mutation, so a mutation may have dropped rows and the row
# count of its original block is not recoverable: `_block_offset` stays unknown. The index has to
# be truncated there - `minmax__block_offset.idx` must not be written past the truncation.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_minmax_prefix_legacy SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_minmax_prefix_legacy (p UInt8, s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only',
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_legacy SELECT 1, toString(number) FROM numbers(9)"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_legacy UPDATE s = 'y' WHERE 1 SETTINGS mutations_sync = 2"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_legacy MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_minmax_prefix_legacy SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_minmax_prefix_legacy"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_legacy UPDATE s = 'x' WHERE 1 SETTINGS mutations_sync = 2"
check_prefix legacy_repair t_minmax_prefix_legacy
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_minmax_prefix_legacy SYNC"

# Same, for a part that covers several blocks: `_block_number` is repaired from the part's own
# block range, `_block_offset` is not repairable, and the index is truncated after it.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_minmax_prefix_merged SYNC"
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_minmax_prefix_merged (p UInt8, s String) ENGINE = MergeTree ORDER BY tuple() PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         part_minmax_index_columns = 'partition_key_only',
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_merged SELECT 1, toString(number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_merged SELECT 1, toString(number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_minmax_prefix_merged SELECT 1, toString(number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_minmax_prefix_merged FINAL"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_merged MODIFY SETTING part_minmax_index_columns = 'with_block_number_offset'"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_minmax_prefix_merged SYNC"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_minmax_prefix_merged"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_minmax_prefix_merged UPDATE s = 'x' WHERE 1 SETTINGS mutations_sync = 2"
check_prefix merged_legacy_repair t_minmax_prefix_merged
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_minmax_prefix_merged SYNC"
