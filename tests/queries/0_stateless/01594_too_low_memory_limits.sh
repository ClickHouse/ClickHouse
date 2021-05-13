#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# it is not mandatory to use existing table since it fails earlier, hence just a placeholder.
# this is format of INSERT SELECT, that pass these settings exactly for INSERT query not the SELECT
${CLICKHOUSE_CLIENT} --format Null --testmode -nm -q "
    insert into placeholder_table_name
    select * from numbers_mt(65535) format Null
    settings max_memory_usage=1, max_untracked_memory=1
    -- { clientError 32 }
"
# check that server is still alive
${CLICKHOUSE_CLIENT} --format Null -q 'SELECT 1'
