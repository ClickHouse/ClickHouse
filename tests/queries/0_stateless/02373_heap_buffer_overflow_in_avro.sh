#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cp $CURDIR/data_avro/corrupted.avro $USER_FILES_PATH/

# Parallel parsing rejects the truncated container in its segmentator, before the decoder this test covers
# gets to it, and reports a different message.
$CLICKHOUSE_CLIENT -q "select * from file(corrupted.avro) settings input_format_parallel_parsing = 0" 2>&1 | grep -F -q "Cannot read compressed data" && echo "OK" || echo "FAIL"
