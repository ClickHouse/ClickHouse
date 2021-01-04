#!/usr/bin/env bash

# shellcheck source=../shell_config.sh
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

TEMP_DIR="$(mktemp -d /tmp/clickhouse.test..XXXXXX)"
cd "${TEMP_DIR:?}"

function cleanup()
{
    rm -fr "${TEMP_DIR:?}"
}
trap cleanup EXIT

# This is random garbage, so compression ratio will be very low.
tr -cd 'a-z0-9' < /dev/urandom | head -c1M > input

# stdin/stdout streams
$CLICKHOUSE_COMPRESSOR < input > output
diff -q <($CLICKHOUSE_COMPRESSOR --decompress < output) input

# positional arguments, and that fact that input/output will be overwritten
$CLICKHOUSE_COMPRESSOR input output
diff -q <($CLICKHOUSE_COMPRESSOR --decompress output) input

# --offset-in-decompressed-block
diff -q <($CLICKHOUSE_COMPRESSOR --decompress --offset-in-decompressed-block 10 output) <(tail -c+$((10+1)) input)

# TODO: --offset-in-compressed-file using some .bin file (via clickhouse-local + check-marks)
