#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-spill-while-used-disk-set-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The set holds 2,000,000 8-byte keys in a table of 32 MiB, and the spill threshold of 88 MiB leaves room to
# spill it, so the set stays in memory while it is built. The query then sorts the rows that pass `IN`, so
# query memory grows by less than 1 MiB with each chunk while the set is used. The `LIMIT` keeps the sort,
# which the outer aggregation would make redundant. The directory and the Bloom filters of the set on disk
# take about 2 MiB, and they count towards the threshold together with the table, which stays in memory until
# the set on disk is complete. So the set spills while it is used before query memory alone exceeds the
# threshold. The test prints the result of the query and whether query memory was below the threshold when the
# set spilled while it was used.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --max_bytes_before_external_set 92274688 --max_bytes_ratio_before_external_set 0 \
    --max_untracked_memory 0 --max_block_size 1024 --max_threads 1 --send_logs_level trace --query "
    SELECT count(), sum(length(s)) FROM
    (
        SELECT number, s FROM (SELECT number, concat(toString(cityHash64(number)), repeat('x', 400)) AS s FROM numbers(300000))
        WHERE number NOT IN (SELECT number * 2 FROM numbers(2000000))
        ORDER BY s LIMIT 300000
    )" 2> "${LOCAL_DIR}/log"

grep -o 'while it is used: .*query memory: [0-9.]* MiB, spill threshold: [0-9.]* MiB' "${LOCAL_DIR}/log" \
    | sed -E 's/.*query memory: ([0-9.]+) MiB, spill threshold: ([0-9.]+) MiB/\1 \2/' \
    | awk '{ print "spilled while used below the threshold", ($1 < $2) }'
