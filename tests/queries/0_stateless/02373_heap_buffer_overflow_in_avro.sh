#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp $CURDIR/data_avro/corrupted.avro $CLICKHOUSE_USER_FILES_UNIQUE/

$CLICKHOUSE_CLIENT -q "select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/corrupted.avro')" 2>&1 | grep -F -q "Cannot read compressed data" && echo "OK" || echo "FAIL"
