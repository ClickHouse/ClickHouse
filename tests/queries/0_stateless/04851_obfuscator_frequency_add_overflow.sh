#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A huge frequency-add value must fail closed with `BAD_ARGUMENTS` instead of silently
# wrapping the `UInt64` frequency counters of the Markov model (which could degenerate
# the model into producing only empty strings). The check lives in `MarkovModel::finalize`,
# which is shared between the `obfuscate` table function and the standalone
# `clickhouse obfuscator` tool, so both surfaces are exercised.
# The frequency cutoff is pinned to 1: with the default cutoff of 5 the tiny training
# histograms would be cleared before the frequency-add step and the overflow would not
# be reached.

DATA_FILE="${CLICKHOUSE_TMP}/04851_data.tsv"
printf 'hello\nworld\n' > "$DATA_FILE"

# CLI surface: the counter addition overflows.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 's String' --input-format TSV --output-format TSV \
    --frequency-cutoff 1 --frequency-add 18446744073709551615 --silent 1 \
    < "$DATA_FILE" 2>&1 | grep -oF "is too large: it overflows the frequency counters" | head -1

# SQL surface: same failure through the `obfuscate_markov_frequency_add` setting.
$CLICKHOUSE_CLIENT --query "
    SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1
    SETTINGS obfuscate_markov_frequency_cutoff = 1, obfuscate_markov_frequency_add = 18446744073709551615
" 2>&1 | grep -oF "is too large: it overflows the frequency counters" | head -1

# Sanity: a large but non-overflowing value still works on both surfaces.
$CLICKHOUSE_OBFUSCATOR --seed test-seed --structure 's String' --input-format TSV --output-format TSV \
    --frequency-cutoff 1 --frequency-add 1000000 --silent 1 < "$DATA_FILE" | wc -l

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM (
        SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 8
        SETTINGS obfuscate_seed = 'stable', obfuscate_markov_frequency_cutoff = 1, obfuscate_markov_frequency_add = 1000000
    );
"

rm -f "$DATA_FILE"
