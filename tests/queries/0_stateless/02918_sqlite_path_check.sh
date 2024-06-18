#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Fast tests don't build external libraries (SQLite)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_exception_message()
{
  $CLICKHOUSE_CLIENT --query "$1" |& head -n1 | sed 's/.*DB::Exception: \(.*\) (version.*/\1/g'
}

get_exception_message "Select * from sqlite('/etc/passwd', 'something');"
get_exception_message "Select * from sqlite('../../../../etc/passwd', 'something');"
