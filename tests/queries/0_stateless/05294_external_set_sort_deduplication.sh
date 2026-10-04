#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-dedup.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Builds a set on disk in a fresh process and prints the probes it finds, whether it spilled, whether its
# temporary files (the runs and the set) satisfy `files`, and the merges of runs counted for the set and for
# sorting. The optional last argument adds checks.
#
# Each subquery starts with keys that no probe finds, which grow the table to the threshold, so the set spills
# with them before the keys of the case arrive. The sorter writes a run once it holds the threshold while query
# memory exceeds it, which exact tracking makes deterministic.
build_set()
{
    local name="$1"
    local threshold="$2"
    local max_block_size="$3"
    local files="$4"
    local rhs="$5"
    local checks="${6:-}"
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_bytes_before_external_set "${threshold}" \
        --max_block_size "${max_block_size}" --max_untracked_memory 0 --multiquery <<SQL
SELECT countIf(number IN (${rhs})) FROM numbers(300000);
SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk';
SELECT sum(value) ${files} FROM system.events WHERE event = 'ExternalSetWritePart';
SELECT (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetMerge'),
    (SELECT sum(value) FROM system.events WHERE event = 'ExternalSortMerge');
${checks}
SQL
}

# 65,536-row chunks of 100 distinct keys each shrink to 100 rows, so the sorter never holds 64 KiB and the set is
# the only file.
build_set within_chunks 65536 65536 "= 1" \
    "SELECT if(number < 65536, 1000000 + number % 2560, number % 100) FROM numbers(1065536)"

# 1,000 chunks hold the same 128 keys, so the sorter writes runs that each hold every key once. All temporary data
# stays far below the 1,024,000 raw key bytes, and the merge of the runs removes the repeats across them.
build_set across_chunks 65536 65536 "> 1" \
    "SELECT if(number < 2560, 1000000 + number, number % 128) FROM numbers(130560) SETTINGS max_block_size = 128" \
    "SELECT sum(value) < 1024000 / 4 FROM system.events WHERE event = 'ExternalSetUncompressedBytes';"

# One-row chunks: 8,200 other keys, of which 8,193 fill the table to the threshold, then 0..998 and 200000, then
# copies of 100000. A merge step emits 0..998 and one 100000, and the next consumes 1,000 copies and emits nothing.
#
# With 1,002 copies, every chunk stays in memory, and the steps belong to the final merge.
build_set duplicate_only_merge_step_in_memory 524288 1000 "= 1" \
    "SELECT multiIf(number < 8200, 1000000000 + number, number < 9199, number % 8200, number = 9199, 200000, 100000)
     FROM numbers(10202) SETTINGS max_block_size = 1"

# With 8,000 copies, the sorter writes runs of more than 1,000 chunks, the steps belong to the merges that write
# them, and each step without rows writes an empty block.
build_set duplicate_only_merge_step_in_run 524288 1000 "> 1" \
    "SELECT multiIf(number < 8200, 1000000000 + number, number < 9199, number % 8200, number = 9199, 200000, 100000)
     FROM numbers(17200) SETTINGS max_block_size = 1"
