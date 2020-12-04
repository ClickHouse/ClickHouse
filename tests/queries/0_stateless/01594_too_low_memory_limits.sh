#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# it is not mandatory to use existing table since it fails earlier, hence just a placeholder.
# this is format of INSERT SELECT, that pass these settings exactly for INSERT query not the SELECT
${CLICKHOUSE_CLIENT} --format Null -q 'insert into placeholder_table_name select * from numbers_mt(65535) format Null settings max_memory_usage=1, max_untracked_memory=1' >& /dev/null
exit_code=$?

# expecting ATTEMPT_TO_READ_AFTER_EOF, 32
test $exit_code -eq 32 || exit 1

# check that server is still alive
${CLICKHOUSE_CLIENT} --format Null -q 'SELECT 1'
