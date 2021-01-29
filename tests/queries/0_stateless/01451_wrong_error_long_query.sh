#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

printf "select 1 in (1, 1, %1048554s  (-1))" " " | ${CLICKHOUSE_CURL} -ss 'http://localhost:8123/?max_query_size=1048576' --data-binary @- | grep -o "Max query size exceeded"
printf "select 1 in (1, 1, %1048554s  (-1))" " " | ${CLICKHOUSE_CURL} -ss 'http://localhost:8123/?max_query_size=1048580' --data-binary @-
