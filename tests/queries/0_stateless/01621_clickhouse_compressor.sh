#!/usr/bin/env bash

# shellcheck source=../shell_config.sh
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# This is random garbage, so compression ratio will be very low.
tr -cd 'a-z0-9' < /dev/urandom | head -c1M > ${CLICKHOUSE_TMP}/input

# stdin/stdout streams
$CLICKHOUSE_COMPRESSOR < ${CLICKHOUSE_TMP}/input > ${CLICKHOUSE_TMP}/output
diff -q <($CLICKHOUSE_COMPRESSOR --decompress < ${CLICKHOUSE_TMP}/output) ${CLICKHOUSE_TMP}/input

# positional arguments, and that fact that input/output will be overwritten
$CLICKHOUSE_COMPRESSOR ${CLICKHOUSE_TMP}/input ${CLICKHOUSE_TMP}/output
diff -q <($CLICKHOUSE_COMPRESSOR --decompress ${CLICKHOUSE_TMP}/output) ${CLICKHOUSE_TMP}/input

# --offset-in-decompressed-block
diff -q <($CLICKHOUSE_COMPRESSOR --decompress --offset-in-decompressed-block 10 ${CLICKHOUSE_TMP}/output) <(tail -c+$((10+1)) ${CLICKHOUSE_TMP}/input)

# TODO: --offset-in-compressed-file using some .bin file (via clickhouse-local + check-marks)
