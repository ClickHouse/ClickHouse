#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# On the `INSERT ... FROM INFILE FORMAT Values` path with the default
# `input_format_values_deduce_templates_of_expressions = 1`, a parse error can be thrown from the
# batched evaluation of templated expressions, after the rows of the block were read. The message of
# such an error must still carry the number of rows the parser has read (` in one of the first N rows`),
# so that schema inference for the parse-error explanation does not sample rows the parser never
# reached: in the data below, the third row is contradictory with the first two for the second column,
# so sampling it would make inference fail and lose the explanation.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/04699_values_template_${CLICKHOUSE_DATABASE}.values"
# The first column is parsed through a deduced template, and its `CAST` to `Date` fails during the
# batched evaluation of the first block (the first two rows). The second column of the third row is
# contradictory with the first two rows, so sampling it would lose the explanation.
printf "('abcd', 'text'), ('efgh', 'text2'), ('2020-01-01', 1)\n" > "$DATA_FILE"

echo "-- the error comes from the batched evaluation of the first block; only the first two rows are sampled"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 1 --max_block_size 2 --max_insert_block_size 2 --query "
    CREATE TABLE test_values_template (a Date, b UInt8) ENGINE = Memory;
    INSERT INTO test_values_template FROM INFILE '${DATA_FILE}' FORMAT Values;
" < /dev/null 2>&1 | check

echo "-- the first two rows match the expected structure (no false positive)"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 1 --max_block_size 2 --max_insert_block_size 2 --query "
    CREATE TABLE test_values_template (a Date, b String) ENGINE = Memory;
    INSERT INTO test_values_template FROM INFILE '${DATA_FILE}' FORMAT Values;
" < /dev/null 2>&1 | check

rm -f "$DATA_FILE"
