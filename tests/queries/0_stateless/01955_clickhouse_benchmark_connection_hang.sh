#!/usr/bin/env bash
# shellcheck disable=SC2086

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Limit number of files to 50, and we will get EMFILE for some of socket()
prlimit --nofile=50 $CLICKHOUSE_BENCHMARK --iterations 1 --concurrency 50 --query 'select 1' 2>&1
echo $?
