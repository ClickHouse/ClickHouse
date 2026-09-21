#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/83577
# An out-of-range `max_read_buffer_size_local_fs` (combined with direct IO) used to be
# passed straight to the allocator while reading the backup metadata, tripping its size
# guard with a `LOGICAL_ERROR` "Too large size passed to allocator". The read buffer size
# is now clamped, so the RESTORE must succeed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists ${CLICKHOUSE_DATABASE}.test;
create table ${CLICKHOUSE_DATABASE}.test (x UInt64, s String) ENGINE = MergeTree() ORDER BY x;
insert into ${CLICKHOUSE_DATABASE}.test select number, toString(number) from numbers(1000);
backup table ${CLICKHOUSE_DATABASE}.test to Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "BACKUP_CREATED"

# Clamping the out-of-range setting emits a `SettingsSanity` warning; silence the server logs
# so the shell test does not fail on non-empty stderr.
${CLICKHOUSE_CLIENT} -m --query "
set send_logs_level = 'error';
set min_bytes_to_use_direct_io = 1, max_read_buffer_size_local_fs = 10000000000000000000;
restore table ${CLICKHOUSE_DATABASE}.test as ${CLICKHOUSE_DATABASE}.test2 from Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}');
" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} --query "select count() from ${CLICKHOUSE_DATABASE}.test2"

${CLICKHOUSE_CLIENT} -m --query "
drop table ${CLICKHOUSE_DATABASE}.test;
drop table ${CLICKHOUSE_DATABASE}.test2;
"
