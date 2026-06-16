#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- the test pins the buffer/prefetch settings, randomization would break it
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up).
# The remote read buffer and the async prefetch buffer are sized from `max_read_buffer_size_remote_fs`
# and `prefetch_buffer_size`, and both are allocated eagerly, so an arbitrarily large configured value
# used to translate into an unbounded allocation. `Context::getReadSettings` now caps both at
# `DBMS_MAX_READ_BUFFER_SIZE` (16 MiB), and `MergeTreePrefetchedReadPool` charges its prefetch memory
# budget with that same effective buffer size. A prefetch-heavy scan over a remote part larger than the
# cap must therefore stay bounded and return the correct result instead of allocating per the
# configured size. The column is ~32 MiB (`CODEC(NONE)`, wide part), so a single-threaded read spans
# several capped 16 MiB buffers, exercising the cap.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0
AS SELECT repeat('a', 1024) FROM numbers_mt(32e3) SETTINGS enable_filesystem_cache = 0;

-- Read the ~32 MiB column through the remote prefetch path with an absurdly large configured buffer.
-- The effective buffer is capped at 16 MiB, so the scan stays bounded and returns the correct result.
SELECT count(), sum(length(s)) FROM test
SETTINGS max_threads = 1,
    remote_filesystem_read_prefetch = 1,
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    max_read_buffer_size_remote_fs = 18446744073709551615,
    prefetch_buffer_size = 18446744073709551615;

DROP TABLE test;
"
