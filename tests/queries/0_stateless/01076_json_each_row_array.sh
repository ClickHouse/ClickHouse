#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "SELECT '[' || arrayStringConcat(arrayMap(x -> '{\"id\": 1, \"name\": \"name1\"}', range(1000000)), ',') || ']'" | ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM table" --input-format JSONEachRow --structure 'id UInt32, name String'
