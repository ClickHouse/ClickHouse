#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-dedup.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Builds a set on disk from the subquery in a fresh process, so that `system.events` counts only this
# build, and prints how many probes the set contains, whether the set spilled to disk, whether the
# number of temporary files it wrote (its runs and the finished set) satisfies `files`, and how many
# times the runs were merged into the set, which counts for the set and not for sorting. The optional
# last argument holds more checks.
#
# Each threshold is below the memory that spilling needs, so the set spills to disk once its table takes
# the threshold. Each subquery starts with keys that no probe finds, which grow the table that far: the
# set moves them to disk before the keys of the case arrive, and the sorter buffers them first. The
# sorter then writes a run once it buffers at least the threshold, while tracked query memory exceeds
# it; exact tracking makes that deterministic. The sorter merges blocks of `max_block_size` rows.
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

# This case has 1,000,000 keys in chunks of 65,536 rows with 100 distinct values, after a chunk with
# 2,560 other keys. Each chunk shrinks to 100 rows before it is buffered, so the sorter never
# buffers 64 KiB although the raw keys take 8 MB: the finished set is the only file.
build_set within_chunks 65536 65536 "= 1" \
    "SELECT if(number < 65536, 1000000 + number % 2560, number % 100) FROM numbers(1065536)"

# This case has 1,000 chunks that each hold the same 128 distinct keys, after 20 chunks with 2,560
# other keys. Nothing is removed within a chunk, so the sorter writes runs. Each run holds every key
# once, so all temporary data (the runs and the finished set) stays far below the 1,024,000 raw key
# bytes, and the merge of the runs removes the repeats across them.
build_set across_chunks 65536 65536 "> 1" \
    "SELECT if(number < 2560, 1000000 + number, number % 128) FROM numbers(130560) SETTINGS max_block_size = 128" \
    "SELECT sum(value) < 1024000 / 4 FROM system.events WHERE event = 'ExternalSetUncompressedBytes';"

# These cases have one-row chunks with the keys 0..998 and 200000, then with the key 100000, after
# one-row chunks with 8,200 other keys, and the table of the set reaches the threshold with 8,193 of
# them. A merge step emits 0..998 and one 100000, the next step consumes 1,000 duplicates of 100000
# and emits no rows, and later steps emit the rest.
#
# With 1,002 chunks of 100000, all the chunks are buffered in memory, and the steps belong to the final merge.
build_set duplicate_only_merge_step_in_memory 524288 1000 "= 1" \
    "SELECT multiIf(number < 8200, 1000000000 + number, number < 9199, number % 8200, number = 9199, 200000, 100000)
     FROM numbers(10202) SETTINGS max_block_size = 1"

# With 8,000 chunks of 100000, the sorter writes runs that each hold more than 1,000 one-row chunks. The steps
# belong to the merges that write the runs, and each step without rows writes an empty block to the file.
build_set duplicate_only_merge_step_in_run 524288 1000 "> 1" \
    "SELECT multiIf(number < 8200, 1000000000 + number, number < 9199, number % 8200, number = 9199, 200000, 100000)
     FROM numbers(17200) SETTINGS max_block_size = 1"
