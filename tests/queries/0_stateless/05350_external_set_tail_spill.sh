#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-tail-spill.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Builds a set of the numbers below 4,500,000, which arrive in order, with the spill threshold of the case in
# a fresh process. Prints the probes that the set finds, then whether it spilled, whether its final merge read
# temporary files, and whether the merge wrote keys in memory to a temporary file first and kept keys in
# memory. The sorter makes runs of 16 MiB, or of the threshold if it is smaller, which stay in memory while
# query memory stays under the threshold. `max_rows_in_set` is the number of keys, so a set that kept a key
# twice would throw. Exact memory tracking makes the spills deterministic, and the other settings fix the
# chunks and the buffers of temporary files.
run_case()
{
    local name="$1"
    local threshold="$2"
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --max_untracked_memory 0 --max_threads 1 \
        --max_block_size 65536 --temporary_files_buffer_size 1048576 --max_rows_in_set 4500000 \
        --max_bytes_before_external_set "${threshold}" --max_bytes_ratio_before_external_set 0 --multiquery <<SQL
SELECT '${name}', countIf(number IN (SELECT number FROM numbers(4500000))) FROM numbers(10000000);
SELECT sumIf(value, event = 'SetsSpilledToDisk'), sumIf(value, event = 'ExternalSetMerge'),
    sumIf(value, event = 'ExternalSetTailSpilledKeys') > 0, sumIf(value, event = 'ExternalSetTailKeptKeys') > 0
FROM system.events;
SQL
}

# With thresholds of 80 and 52 MiB, the set spills when its table of 32 MiB would grow, and the sorter makes 3
# runs, the last of which holds the rest of the keys. With 80 MiB, the runs fit in memory beside the merge,
# which reads no temporary files.
run_case kept 83886080

# With 52 MiB, the merge first writes the first run to a temporary file, which leaves room for the other two.
# It reads the file and then the runs in memory, as one chain.
run_case partial 54525952

# With 1 MiB, the runs go to temporary files as they form, and the merge writes the last keys in memory to a
# temporary file as well.
run_case written 1048576
