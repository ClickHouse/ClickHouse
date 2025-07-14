#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --stacktrace -q "select '{\"a\" : 4ab2}'::JSON" 2>&1 | grep -c -F "SimdJSON"
$CLICKHOUSE_LOCAL --allow_experimental_json_type=1 --allow_simdjson=0 --stacktrace -q "select '{\"a\" : 4ab2}'::JSON" 2>&1 | grep -c -F "RapidJSON"


