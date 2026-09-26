#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses a filesystem cache disk

# `boundary_alignment = max_file_segment_size` is load-bearing: it makes
# `roundUpToMultiple(downloaded_size, boundary_alignment)` reach the whole segment range, so
# `FileSegment::shrinkFileSegmentToDownloadedSize` early-returns and the segment stays
# PARTIALLY_DOWNLOADED with a usable prefix. `background_download_threads = 0` keeps it that way and
# `cache_on_write_operations = 0` keeps the INSERT from pre-filling the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every setting the test depends on is pinned at query level (which beats the runner's
# randomization), so no no-random-settings tag is needed. Two of them are worth spelling out:
# `optimize_read_in_order` has to stay on, otherwise the `LIMIT` below reads the whole column instead
# of stopping part way through and every segment ends up DOWNLOADED; and `use_uncompressed_cache` has
# to stay off, otherwise the second read is served from the uncompressed cache and never reaches the
# filesystem cache at all (both byte counters stay at 0).
READ_SETTINGS="max_threads = 1, remote_filesystem_read_prefetch = 0,
    remote_filesystem_read_method = 'read', filesystem_cache_segments_batch_size = 0,
    max_block_size = 8192, read_through_distributed_cache = 0, enable_filesystem_cache = 1,
    optimize_read_in_order = 1, use_uncompressed_cache = 0"

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists test;
create table test (a UInt64, b String) ENGINE = MergeTree() ORDER BY a
settings disk = disk(
    type = cache,
    name = '${CLICKHOUSE_TEST_UNIQUE_NAME}',
    path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/',
    max_size = '4Gi',
    max_file_segment_size = '32Mi',
    boundary_alignment = '32Mi',
    background_download_threads = 0,
    cache_on_write_operations = 0,
    disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_blob/')),
  min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
insert into test select number, randomPrintableASCII(300) from numbers(400000)
settings max_insert_block_size = 200000;
"

# The cache is freshly created with a per-database unique name, so it starts empty (no need to drop
# it). Fill a prefix only: the LIMIT read stops in the middle of a 32Mi segment.
${CLICKHOUSE_CLIENT} --query "
select sum(length(b)) from (select b from test order by a limit 60000) format Null
settings read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, ${READ_SETTINGS};
"

# Fixture liveness: without a PARTIALLY DOWNLOADED segment carrying a usable prefix the oracle below
# would pass vacuously. Note the state is spelled with a SPACE in system.filesystem_cache.
echo -n 'partially downloaded segment with downloaded bytes exists: '
${CLICKHOUSE_CLIENT} --query "
select count() > 0 from system.filesystem_cache
where cache_name = '${CLICKHOUSE_TEST_UNIQUE_NAME}'
  and state = 'PARTIALLY DOWNLOADED' and downloaded_size > 0;
"

# The same read again, now in passive mode. Compare the two counters instead of printing them: the
# raw values depend on the block size and the build.
query_id="04757-${CLICKHOUSE_DATABASE}-passive"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
select sum(length(b)) from (select b from test order by a limit 60000) format Null
settings read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1, ${READ_SETTINGS};
"

${CLICKHOUSE_CLIENT} --query "system flush logs query_log"

echo -n 'passive read served more bytes from the cache than from the source: '
${CLICKHOUSE_CLIENT} --query "
select ProfileEvents['CachedReadBufferReadFromCacheBytes']
     > ProfileEvents['CachedReadBufferReadFromSourceBytes']
from system.query_log
where query_id = '$query_id' and type = 'QueryFinish' and current_database = currentDatabase();
"

echo -n 'passive read fetched nothing from the source: '
${CLICKHOUSE_CLIENT} --query "
select ProfileEvents['CachedReadBufferReadFromSourceBytes'] = 0
from system.query_log
where query_id = '$query_id' and type = 'QueryFinish' and current_database = currentDatabase();
"

# Passive mode must never populate the cache. CachedReadBufferCacheWriteBytes is incremented only in
# CachedOnDiskReadBufferFromFile::writeCache, so 0 pins the second half of the setting's contract.
echo -n 'passive read wrote nothing to the cache: '
${CLICKHOUSE_CLIENT} --query "
select ProfileEvents['CachedReadBufferCacheWriteBytes'] = 0
from system.query_log
where query_id = '$query_id' and type = 'QueryFinish' and current_database = currentDatabase();
"

# Correctness control. A full-table read has to leave the cached prefix half way through, which
# exercises the CACHED -> bypass switchover in updateReadStateIfNeeded. The table is filled with
# random data, so the checksum itself is not reproducible - what is asserted is that all three arms
# return the same one.
results=()
for cache_settings in \
    "enable_filesystem_cache = 0" \
    "enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0" \
    "enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1"
do
    results+=("$(${CLICKHOUSE_CLIENT} --query "
    select sum(cityHash64(b)), count() from test
    settings max_threads = 1, read_through_distributed_cache = 0, $cache_settings;
    ")")
done

echo -n 'full table read agrees across cache disabled / normal mode / passive mode: '
if [ "${results[0]}" = "${results[1]}" ] && [ "${results[1]}" = "${results[2]}" ]; then
    echo 1
else
    echo "0 -- ${results[0]} | ${results[1]} | ${results[2]}"
fi

echo -n 'full table row count: '
echo "${results[0]}" | cut -f2

${CLICKHOUSE_CLIENT} --query "drop table test;"
