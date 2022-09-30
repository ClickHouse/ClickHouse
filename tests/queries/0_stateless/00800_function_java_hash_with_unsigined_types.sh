#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "SELECT javaHash(toUInt8(1))" |& {
    grep -F  'DB::Exception:'  | wc -l
}
${CLICKHOUSE_CLIENT} -q "SELECT javaHash(toUInt16(1))" |& {
    grep -F  'DB::Exception:'  | wc -l
}
${CLICKHOUSE_CLIENT} -q "SELECT javaHash(toUInt32(1))" |& {
    grep -F  'DB::Exception:'  | wc -l
}
${CLICKHOUSE_CLIENT} -q "SELECT javaHash(toUInt64(1))" |& {
    grep -F  'DB::Exception:'  | wc -l
}
