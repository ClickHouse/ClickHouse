#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The standalone `clickhouse obfuscator` tool must enforce the same parameter contract
# as the `obfuscate` table function (they share `MarkovModelParameters::validate`).

DATA_FILE="${CLICKHOUSE_TMP}/04846_data.tsv"
EMPTY_FILE="${CLICKHOUSE_TMP}/04846_empty.tsv"
printf '1\n' > "$DATA_FILE"
: > "$EMPTY_FILE"

# Zero Markov model order is rejected instead of reaching a logical error.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x String' --input-format TSV --output-format TSV --order 0 --silent 1 \
    < "$DATA_FILE" 2>&1 | grep -oF "The option '--order' must be greater than zero"

# An absurdly large order is rejected instead of failing from the allocation path.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x String' --input-format TSV --output-format TSV --order 100000 --silent 1 \
    < "$DATA_FILE" 2>&1 | grep -oF "The option '--order' must not exceed 1000, got 100000"

# A frequency desaturation factor outside [0, 1] is rejected instead of driving
# bucket weights through a negative-to-`UInt64` conversion.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x String' --input-format TSV --output-format TSV --frequency-desaturate 2 --silent 1 \
    < "$DATA_FILE" 2>&1 | grep -oF "The option '--frequency-desaturate' must be in the range [0, 1]"

# An explicit --limit over an empty input fails closed instead of rebuilding the
# input pipeline forever.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x UInt8' --input-format TSV --output-format TSV --limit 1 --silent 1 \
    < "$EMPTY_FILE" 2>&1 | grep -oF "a full generation pass over the input produced no rows"

# Sanity: multi-pass amplification beyond the source size still works.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x UInt8' --input-format TSV --output-format TSV --limit 5 --silent 1 \
    < "$DATA_FILE" | wc -l

rm -f "$DATA_FILE" "$EMPTY_FILE"
