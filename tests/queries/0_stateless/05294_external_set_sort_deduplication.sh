#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-dedup.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Runs the queries of a case in a fresh process, then prints the counts of spilled sets, of their temporary
# files (the runs of the sorter and the sets) and of merges of runs in files. Each case sets `max_rows_in_set`
# to the number of distinct keys of its set, so a set that kept a key twice would exceed it and its query
# would throw. Exact memory tracking makes the spills deterministic, and one thread keeps the chunks in order.
run_case()
{
    local name="$1"
    local queries="$2"
    shift 2
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_untracked_memory 0 --max_threads 1 "$@" --multiquery <<SQL
${queries}
SELECT '${name}', (SELECT sum(value) FROM system.events WHERE event = 'SetsSpilledToDisk'),
    (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetWritePart'),
    (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetMerge');
SQL
}

# Chunks of 8,192 rows repeat the same 100 keys. The sorter sorts its buffer whenever it holds a run of
# 16 KiB, and each sort leaves fewer keys than half a run, which stay for the next sort. So the sorter makes
# no run while the keys arrive, and only the merge writes them to a temporary file, as one run, because query
# memory exceeds the threshold. The subquery starts with keys that no probe finds, which grow the table to
# the threshold, so the set spills before the repeated keys arrive.
run_case buffer_stays "
SELECT countIf(number IN (SELECT if(number < 8192, 1000000 + number % 600, number % 100) FROM numbers(40960)))
FROM numbers(1000);" \
    --max_bytes_before_external_set 16384 --max_block_size 8192 --max_rows_in_set 700

# With a threshold of 1 byte, the set spills at its first chunk, and each chunk becomes a run in a file of its
# own. Chunk `k` holds the keys from `32 * k` to `32 * k + 63`, so each run repeats half of the keys of the
# previous one. The runs make two chains, and their merge passes each key once.
run_case repeats_across_runs "
SELECT countIf(number IN (SELECT intDiv(number, 64) * 32 + number % 64 FROM numbers(1024))) FROM numbers(1000);" \
    --max_bytes_before_external_set 1 --max_block_size 64 --max_rows_in_set 544

# Each key but the first comes twice in a row, so each chunk ends with the key that the next one starts with.
# The sorted runs make one chain, which passes the key at each boundary once. With a threshold of 16 bytes,
# the sorter sorts its buffer whenever it holds 2 keys, so each chunk of 64 rows becomes a run in a file, and
# the last chunk of 1 key stays in memory until the merge writes it to a file of its own, because query memory
# exceeds the threshold. The chain reads that file last, and it holds only the key that ends the previous run.
run_case touching_runs "
SELECT countIf(number IN (SELECT intDiv(number + 1, 2) FROM numbers(1025))) FROM numbers(1000);" \
    --max_bytes_before_external_set 16 --max_block_size 64 --max_rows_in_set 513

# A run that ends with the largest key of its type ends a chain that no later run joins. The merge treats a
# chain that has ended as holding the largest key, and still passes a real largest key once. The chunks hold
# the keys from 0 to 62 and the largest key, from 32 to 94 and the largest key, and from 64 to 127, whose
# chain ends first. The probes are the smallest and the largest keys of the range, the next key, and the two
# largest keys of the type. The merge keeps whole 64-bit keys in its tree, and one word of 256-bit keys, which
# it completes with the whole keys when the words are equal.
run_case largest_key "
SELECT 'UInt64', groupArray(k IN (
    SELECT if(intDiv(number, 64) < 2 AND number % 64 = 63, 18446744073709551615, intDiv(number, 64) * 32 + number % 64)
    FROM numbers(192)))
FROM (SELECT arrayJoin([0, 127, 128, 18446744073709551614, 18446744073709551615]) AS k);
SELECT 'UInt256', groupArray(k IN (
    SELECT if(intDiv(number, 64) < 2 AND number % 64 = 63, bitNot(toUInt256(0)), toUInt256(intDiv(number, 64) * 32 + number % 64))
    FROM numbers(192)))
FROM (SELECT arrayJoin([toUInt256(0), 127, 128, bitNot(toUInt256(1)), bitNot(toUInt256(0))]) AS k);" \
    --max_bytes_before_external_set 1 --max_block_size 64 --max_rows_in_set 129
