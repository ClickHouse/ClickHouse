#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp $CURDIR/data_avro/corrupted.avro $USER_FILES_PATH/

$CLICKHOUSE_CLIENT -q "select * from file(corrupted.avro)" 2>&1 | grep -F -q "Cannot read compressed data" && echo "OK" || echo "FAIL"
