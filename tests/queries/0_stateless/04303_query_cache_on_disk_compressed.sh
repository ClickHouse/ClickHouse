#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal query result cache

# Regression test: reading a *compressed* entry back from the query result cache on disk must return
# decompressed chunks. Before the fix, the disk-read path compressed the deserialized entry and handed the
# resulting `ColumnCompressed` chunks straight to the pipeline (unlike the memory-read path, which
# decompresses first), so a disk hit threw "ColumnCompressed must be decompressed before use" (an exception
# that aborts the server under `abort_on_logical_error`). The result must be large and compressible enough
# that `query_cache_compress_entries = 1` actually produces a `ColumnCompressed`: a column block must exceed
# `min_size_to_compress` (4 KiB) and compress by at least 50%. Small results are wrapped without real
# compression, which is why 02494_query_cache_on_disk does not catch this.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="use_query_cache = true, enable_writes_to_query_cache_disk = true, enable_reads_from_query_cache_disk = true, query_cache_compress_entries = true"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t(id Int64, c String) ENGINE = MergeTree ORDER BY id"
# Highly compressible: identical long strings.
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number, repeat('abcabcabc', 1000) FROM numbers(5000)"

# Ground truth without the cache.
truth=$(${CLICKHOUSE_CLIENT} --query "SELECT id, c FROM t ORDER BY id FORMAT TSV SETTINGS use_query_cache = 0" | md5sum)

# First run populates both the memory and the disk cache.
first=$(${CLICKHOUSE_CLIENT} --query "SELECT id, c FROM t ORDER BY id FORMAT TSV SETTINGS ${SETTINGS}" | md5sum)

# Drop only the memory cache; the entry must now live on disk only.
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE TYPE 'Memory'"
echo "-- entry type after dropping memory cache:"
${CLICKHOUSE_CLIENT} --query "SELECT type FROM system.query_cache WHERE query LIKE 'SELECT id, c FROM t ORDER BY id%'"

# This run must be served from disk and must return the correct (decompressed) result.
disk=$(${CLICKHOUSE_CLIENT} --query "SELECT id, c FROM t ORDER BY id FORMAT TSV SETTINGS ${SETTINGS}" | md5sum)

echo "-- disk-read result matches ground truth:"
if [ "$disk" = "$truth" ] && [ "$first" = "$truth" ]; then echo "yes"; else echo "no ($truth / $first / $disk)"; fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
