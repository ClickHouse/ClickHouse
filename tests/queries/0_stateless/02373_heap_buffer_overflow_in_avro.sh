#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp "$CURDIR"/data_avro/corrupted.avro "$USER_FILES_PATH"/

if $CLICKHOUSE_CLIENT -q "select * from file(corrupted.avro)" >/dev/null 2>&1; then
    echo "FAIL"
elif $CLICKHOUSE_CLIENT -q "SELECT 1" >/dev/null 2>&1; then
    echo "OK"
else
    echo "FAIL"
fi
