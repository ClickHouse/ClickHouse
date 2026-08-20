#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Build three 64 MiB files in `user_files`. The size is chosen so that the
# `ColumnString::Chars` capacity (a power of two) crosses 128 MiB while reading
# the third file: `chars` reaches 192 MiB content but the previous capacity was
# 128 MiB, so it reallocates to 256 MiB. While that 256 MiB chunk is being
# allocated, the previous 128 MiB buffer is still alive, and so is the per-row
# `ReadBufferFromFile` (1 MiB). On the pre-fix code path, the file content also
# stages through a `std::string` first, which pins another 64 MiB before the
# column reallocation fires -- pushing the peak past the limit set below.
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
for i in 1 2 3; do
    head -c 67108864 /dev/zero > "${CLICKHOUSE_USER_FILES_UNIQUE}/big_${i}.bin"
done

TEST_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Empirical peaks at this scenario (aarch64, single stream):
#   * Streaming directly into the column (post-fix): ~385 MiB
#       1 MiB (read buffer) + 128 MiB (old chars) + 256 MiB (new chars)
#   * Staging through `std::string` (pre-fix):       ~449 MiB
#       1 MiB + 64 MiB (`std::string`) + 128 MiB (old chars) + 256 MiB (new chars)
# A 440 MB (~419 MiB) limit lies comfortably between the two peaks, so the
# pre-fix path raises `MEMORY_LIMIT_EXCEEDED` while the post-fix path completes
# successfully. `max_threads=1` pins the work to one stream so the peaks above
# are deterministic instead of being divided across `num_streams` columns.
$CLICKHOUSE_CLIENT --max_threads=1 --max_memory_usage=440000000 --query "
    SELECT sum(length(content))
    FROM filesystem('${TEST_REL}')
    WHERE name LIKE 'big_%'
" 2>&1 | grep -F -q MEMORY_LIMIT_EXCEEDED && echo 1 || echo 0

# Sanity check: with a generous limit, the same query succeeds and reports the
# correct total size, so the streaming path produces the same bytes as before.
$CLICKHOUSE_CLIENT --max_threads=1 --query "
    SELECT sum(length(content))
    FROM filesystem('${TEST_REL}')
    WHERE name LIKE 'big_%'
"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
