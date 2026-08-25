#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses a filesystem cache disk

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/83577 (a continuation of 04546).
# `prefetch_buffer_size` feeds `ReadSettings::remote_fs_settings.large_buffer_size`, which is only
# consumed when the filesystem cache prefers a bigger buffer: `DiskObjectStorage::prepareReadPipeline`
# raises the remote read buffer size to `large_buffer_size` when reading through a cache-backed
# object storage disk. This test exercises exactly that path with out-of-range values of
# `prefetch_buffer_size` and `max_read_buffer_size_remote_fs` (clamped by `doSettingsSanityCheckClamp`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists ${CLICKHOUSE_DATABASE}.test;
create table ${CLICKHOUSE_DATABASE}.test (a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple()
settings disk = disk(
    type = cache,
    name = '${CLICKHOUSE_TEST_UNIQUE_NAME}',
    path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/',
    max_size = '1Gi',
    disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_blob/'));
insert into ${CLICKHOUSE_DATABASE}.test select number, toString(number) from numbers(100000);
"

# Clamping the out-of-range settings emits a `SettingsSanity` warning; silence the server logs
# so the shell test does not fail on non-empty stderr.
${CLICKHOUSE_CLIENT} -m --query "
set send_logs_level = 'error';
select sum(a), count() from ${CLICKHOUSE_DATABASE}.test
settings prefetch_buffer_size = 10000000000000000000, max_read_buffer_size_remote_fs = 10000000000000000000,
         remote_filesystem_read_prefetch = 1, remote_filesystem_read_method = 'threadpool',
         enable_filesystem_cache = 1, filesystem_cache_prefer_bigger_buffer_size = 1,
         read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, read_through_distributed_cache = 0;
"

${CLICKHOUSE_CLIENT} --query "drop table ${CLICKHOUSE_DATABASE}.test;"
