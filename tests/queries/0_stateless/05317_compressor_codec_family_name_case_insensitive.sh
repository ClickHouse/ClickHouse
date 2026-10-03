#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.data"
compressed="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.compressed"

seq 1 1000 > "$data"

# A codec family name is matched case-insensitively, the same way as in `CODEC(...)`.
$CLICKHOUSE_COMPRESSOR --codec 'delta' --codec 'zstd(3)' --input "$data" --output "$compressed"
$CLICKHOUSE_COMPRESSOR --decompress --input "$compressed" | cmp - "$data" && echo "roundtrip ok"
$CLICKHOUSE_COMPRESSOR --stat --input "$compressed" > /dev/null && echo "stat ok"

# `Quantized` only works in a column definition, so it is rejected here in any spelling.
$CLICKHOUSE_COMPRESSOR --codec "Quantized('int8', 64)" --input "$data" --output "$compressed" 2>&1 | grep -c "can only be specified in the column definition"
$CLICKHOUSE_COMPRESSOR --codec 'LZ4' --codec "quantized('int8', 64)" --input "$data" --output "$compressed" 2>&1 | grep -c "can only be specified in the column definition"

rm -f "$data" "$compressed"
