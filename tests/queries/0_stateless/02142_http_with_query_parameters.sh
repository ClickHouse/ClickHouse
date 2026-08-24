#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo '
SELECT
   sum(toUInt8(1) ? toUInt8(1) : toUInt8(1)) AS metric,
   groupArray(toUInt8(1) ? toUInt8(1) : toUInt8(1)),
   groupArray(toUInt8(1) ? toUInt8(1) : 1),
   sum(toUInt8(1) ? toUInt8(1) : 1)
FROM (SELECT materialize(toUInt64(1)) as key FROM numbers(22))
WHERE key = {b1:Int64}' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&param_b1=1" -d @-
