#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

opts=(
    --input-format CSV
    -q 'SELECT number FROM numbers(2)'
)

echo 'TSVWithNames'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNames

echo 'TSVWithNamesAndTypes'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNamesAndTypes

echo 'CSVWithNames'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format CSVWithNames
