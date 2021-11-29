#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test is pretty depend on query execution time
# In release, it works about 14 seconds with no timeout.

function run
{
  ${CLICKHOUSE_CLIENT} -q "select number from remote('127.0.0.2', numbers(10)) where number global in (select number + 9 from numbers(400000000)) settings max_execution_time=3" 2>&1 | cat > /dev/null
}

export -f run
timeout 10 bash -c run
