#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/83577
# An out-of-range `max_read_buffer_size_remote_fs` or `prefetch_buffer_size` is clamped by
# `doSettingsSanityCheckClamp`, same as `max_read_buffer_size` / `max_read_buffer_size_local_fs`
# (covered by 04535/04536). This test exercises the remote-fs and prefetch code paths
# specifically, using a `local_blob_storage` disk (an object-storage-shaped disk backed by the
# local filesystem) so it does not require a real object storage service.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists ${CLICKHOUSE_DATABASE}.test;
create table ${CLICKHOUSE_DATABASE}.test (a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple()
settings disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/');
insert into ${CLICKHOUSE_DATABASE}.test select number, toString(number) from numbers(100000);
"

# Clamping the out-of-range setting emits a `SettingsSanity` warning; silence the server logs
# so the shell test does not fail on non-empty stderr.
${CLICKHOUSE_CLIENT} -m --query "
set send_logs_level = 'error';
select sum(a), count() from ${CLICKHOUSE_DATABASE}.test
settings max_read_buffer_size_remote_fs = 10000000000000000000;
"

${CLICKHOUSE_CLIENT} -m --query "
set send_logs_level = 'error';
select sum(a), count() from ${CLICKHOUSE_DATABASE}.test
settings prefetch_buffer_size = 10000000000000000000, remote_filesystem_read_prefetch = 1;
"

${CLICKHOUSE_CLIENT} --query "drop table ${CLICKHOUSE_DATABASE}.test;"
