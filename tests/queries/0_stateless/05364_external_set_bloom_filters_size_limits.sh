#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set-bloom-filters-size-limits.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The Bloom filters of a set on disk take about 1 byte per key that they are sized for. The subquery has
# 1,000,000 distinct hashes. A set that reaches its size limits in the `break` mode keeps the keys within them
# and at most one merged batch of 65,536 keys more, and its filters are sized for these keys. With the limit
# on rows, the set keeps the first batch. With the limit on bytes, which counts 8 bytes for each key, it keeps
# two. Filters sized for all the keys of the subquery would take 8 to 15 bytes per key that these sets keep.
# The test prints the keys that the lookups find, and for each set on disk, the keys that it keeps and whether
# its filters take less than 2 bytes per key.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --max_bytes_before_external_set 1 --max_untracked_memory 0 --max_block_size 65536 \
    --send_logs_level trace --multiquery 2> "${LOCAL_DIR}/log" <<'SQL'
SELECT 'rows limit', count() FROM numbers(1000000) WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(1000000))
SETTINGS max_rows_in_set = 100, set_overflow_mode = 'break';
SELECT 'bytes limit', count() FROM numbers(1000000) WHERE cityHash64(number) IN (SELECT cityHash64(number) FROM numbers(1000000))
SETTINGS max_bytes_in_set = 800000, set_overflow_mode = 'break';
SQL

grep -o 'Created set on disk with [0-9]* keys .* [0-9]* bytes of filters' "${LOCAL_DIR}/log" \
    | sed -E 's/.* with ([0-9]+) keys .* ([0-9]+) bytes of filters/\1 \2/' \
    | awk '{ print $1, ($2 < 2 * $1) }'
