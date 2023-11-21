#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function get_exception_message()
{
  $CLICKHOUSE_CLIENT --query "$1" |& grep -o 'Path must be inside user-files path'
}

get_exception_message "create database db_filesystem ENGINE=Filesystem('/etc');"
get_exception_message "create database db_filesystem ENGINE=Filesystem('../../../../../../../../etc')';"