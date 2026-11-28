#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# `INSERT ... FROM INFILE` re-reads the file to infer its structure, so it must honour the same
# row bound as the inline/stdin/server paths: only the rows the parser had actually reached may be
# sampled. Otherwise a later row the parser never got to could widen the inferred type of a column
# and turn a value-level parse error in an earlier row into a bogus "structure mismatch"
# explanation. The bound is taken from the `(at row N)` part of the parse error.
#
# The bound holds regardless of `input_format_parallel_parsing`: `ParallelParsingInputFormat` gives
# every unit's parser the number of rows read before it, so `(at row N)` counts rows from the
# beginning of the file in both cases. Both values of the setting are checked.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE_LATER_ROW="${CLICKHOUSE_TMP}/04643_infile_later_row_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_MISMATCH="${CLICKHOUSE_TMP}/04643_infile_mismatch_${CLICKHOUSE_DATABASE}.tsv"

printf '1\t1.5\n2\ttext\n' > "$DATA_FILE_LATER_ROW"
printf 'text\t1\n' > "$DATA_FILE_MISMATCH"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_infile_row_bound (a UInt8, b UInt8) ENGINE = Memory"

for parallel_parsing in 0 1
do
    echo "-- input_format_parallel_parsing = ${parallel_parsing}"

    echo "-- the first row fails on a value; the conflicting second row is not sampled (no false positive)"
    $CLICKHOUSE_CLIENT --input_format_parallel_parsing "${parallel_parsing}" \
        --query "INSERT INTO test_infile_row_bound FROM INFILE '${DATA_FILE_LATER_ROW}' FORMAT TSV" < /dev/null 2>&1 | check

    echo "-- the failing row itself is sampled: text where a number is expected (explanation still fires)"
    $CLICKHOUSE_CLIENT --input_format_parallel_parsing "${parallel_parsing}" \
        --query "INSERT INTO test_infile_row_bound FROM INFILE '${DATA_FILE_MISMATCH}' FORMAT TSV" < /dev/null 2>&1 | check
done

$CLICKHOUSE_CLIENT -q "DROP TABLE test_infile_row_bound"
rm -f "$DATA_FILE_LATER_ROW" "$DATA_FILE_MISMATCH"
