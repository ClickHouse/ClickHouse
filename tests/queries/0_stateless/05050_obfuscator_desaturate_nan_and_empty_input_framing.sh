#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `MarkovModelParameters::validate` is shared between the `obfuscate` table function and the
# standalone `clickhouse obfuscator` tool, so both surfaces must reject a NaN desaturation factor:
# NaN compares false with both bounds of the documented [0, 1] range and would otherwise be
# silently accepted as a no-op.

DATA_FILE="${CLICKHOUSE_TMP}/05050_data.tsv"
EMPTY_FILE="${CLICKHOUSE_TMP}/05050_empty.tsv"
OUT_FILE="${CLICKHOUSE_TMP}/05050_out.json"
printf 'hello\nworld\n' > "$DATA_FILE"
: > "$EMPTY_FILE"

# CLI surface.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 's String' --input-format TSV --output-format TSV --frequency-desaturate nan --silent 1 \
    < "$DATA_FILE" 2>&1 | grep -oF "The option '--frequency-desaturate' must be in the range [0, 1]"

# SQL surface: the generic settings layer already rejects a non-finite float value before it can
# reach the shared validator, so the setting can never be NaN.
$CLICKHOUSE_CLIENT --query "
    SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1
    SETTINGS obfuscate_markov_frequency_desaturate = nan
" 2>&1 | grep -oF "Float setting value must be finite" | head -1

# An explicit --limit over an empty input must fail closed without touching the output: for a format
# with non-trivial framing such as JSON, no prefix/suffix may be emitted that would look like a
# syntactically valid empty result.
# Redirection order matters: stderr is routed into the pipe first, then stdout into the file.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 'x UInt8' --input-format TSV --output-format JSON --limit 3 --silent 1 \
    < "$EMPTY_FILE" 2>&1 > "$OUT_FILE" \
    | grep -oF "a full generation pass over the input produced no rows"
echo -n "output bytes: "
wc -c < "$OUT_FILE"

rm -f "$DATA_FILE" "$EMPTY_FILE" "$OUT_FILE"
