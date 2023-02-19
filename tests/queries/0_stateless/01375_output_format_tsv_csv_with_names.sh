#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

opts=(
    -q 'SELECT number FROM numbers(2)'
)

echo 'TSVWithNames'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNames

echo 'TSVWithNamesAndTypes'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNamesAndTypes

echo 'TSVRawWithNames'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNames

echo 'TSVRawWithNamesAndTypes'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format TSVWithNamesAndTypes

echo 'CSVWithNames'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format CSVWithNames

echo 'CSVWithNamesAndTypes'
${CLICKHOUSE_LOCAL} "${opts[@]}" --format CSVWithNamesAndTypes
