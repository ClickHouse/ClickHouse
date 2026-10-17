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
# with them before the keys of the case arrive. The sorter writes a run once it holds the threshold while
# query memory exceeds it, which exact tracking makes deterministic.
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
SELECT countIf(number IN (${rhs})) FROM numbers(1000);
SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk';
SELECT sum(value) ${files} FROM system.events WHERE event = 'ExternalSetWritePart';
SELECT (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetMerge'),
    (SELECT sum(value) FROM system.events WHERE event = 'ExternalSortMerge');
${checks}
SQL
}

# 8,192-row chunks of 100 distinct keys each shrink to 100 rows, so the sorter never holds 16 KiB and the set
# is the only file.
build_set within_chunks 16384 8192 "= 1" \
    "SELECT if(number < 8192, 1000000 + number % 600, number % 100) FROM numbers(40960)"

# 100 chunks hold the same 128 keys, so the sorter writes runs that each hold every key once. All temporary
# data stays far below the 102,400 raw key bytes, and the merge of the runs removes the repeats across them.
build_set across_chunks 16384 16384 "> 1" \
    "SELECT if(number < 640, 1000000 + number, number % 128) FROM numbers(13440) SETTINGS max_block_size = 128" \
    "SELECT sum(value) < 102400 / 4 FROM system.events WHERE event = 'ExternalSetUncompressedBytes';"

# The first row brings 2,100 other keys in one chunk, which fill the table to the threshold. Then one-row
# chunks bring 0..98 and 300, then copies of 200. A merge step of 100 rows emits 0..98 and one 200, and the
# next consumes 100 copies and emits nothing.
#
# With 102 copies, every chunk stays in memory, and the steps belong to the final merge.
build_set duplicate_only_merge_step_in_memory 65536 100 "= 1" \
    "SELECT arrayJoin(if(number = 0, range(1000000, 1002100), [multiIf(number < 100, number - 1, number = 100, 300, 200)]))
     FROM numbers(203) SETTINGS max_block_size = 1"

# With 1,000 copies, the sorter writes a run of more than 100 chunks, the steps belong to the merge that
# writes it, and the steps without rows write no block.
build_set duplicate_only_merge_step_in_run 65536 100 "> 1" \
    "SELECT arrayJoin(if(number = 0, range(1000000, 1002100), [multiIf(number < 100, number - 1, number = 100, 300, 200)]))
     FROM numbers(1101) SETTINGS max_block_size = 1"
