#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Numeric-backed special types (Enum, Time, Time64, Interval) have no natural Hive text
# representation: Hive has no corresponding type. They must throw NOT_IMPLEMENTED for HiveText
# output instead of silently emitting their raw underlying numeric value (the enum's number
# rather than its name, the time as raw seconds, and so on).
for expr in \
    "CAST('a', 'Enum8(''a'' = 1)')" \
    "CAST(1, 'Time')" \
    "CAST(1, 'Time64(3)')" \
    "INTERVAL 1 SECOND"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${expr} FORMAT HiveText" 2>&1 | grep -o -m1 "Type [A-Za-z0-9]* is not supported by the HiveText output format"
done
