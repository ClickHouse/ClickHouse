#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This is the regression test for clickhouse-local, that may use already freed
# context from the suggestion thread on error.

grep_options=(
    -e ^$
    -e 'Cannot create file: /no/such/directory'
    -e 'Cannot load data for command line suggestions:'
    -e 'ClickHouse local version'
)

ASAN_OPTIONS=$ASAN_OPTIONS:exitcode=3 $CLICKHOUSE_LOCAL --history_file /no/such/directory |& grep -v "${grep_options[@]}"
# on sanitizer error the code will be not 1, but 3
echo $?
