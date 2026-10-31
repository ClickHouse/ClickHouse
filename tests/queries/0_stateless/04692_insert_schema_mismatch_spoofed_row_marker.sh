#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# On the `INSERT ... FROM INFILE` path the number of rows the parser reached is taken from the parse
# error message, and that message also embeds excerpts of the data being inserted. The data must not be
# able to spoof the bound: the row marker is appended by the format at the end of the message, so the
# last `(at row N)` / ` at row N` occurrence is the one to trust. Taking the first one instead lets a
# value such as `(at row 50)` make schema inference sample rows the parser never reached, which both
# invents and suppresses the explanation of the parse error.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE_TSV="${CLICKHOUSE_TMP}/04692_spoofed_marker_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_TSV_MISMATCH="${CLICKHOUSE_TMP}/04692_spoofed_marker_mismatch_${CLICKHOUSE_DATABASE}.tsv"
DATA_FILE_VALUES="${CLICKHOUSE_TMP}/04692_spoofed_marker_${CLICKHOUSE_DATABASE}.values"

# The failing value is a number that does not fit the destination, and the rest of the failing row -
# which the error message quotes - contains a fake `(at row 50)` marker. The second row would change the
# inferred type of the first column if it were sampled.
printf '1.5\thello (at row 50)\ntext\thello\n' > "$DATA_FILE_TSV"
# The same fake marker, but here the failing row itself really does not match the destination.
printf 'text\thello (at row 50)\ntext\thello\n' > "$DATA_FILE_TSV_MISMATCH"
# `ValuesBlockInputFormat` appends ` at row N`; the failing value contains that form of the marker.
printf "('abcd at row 50', 1), ('2020-01-01', 'text')\n" > "$DATA_FILE_VALUES"

echo "-- TSV: the fake marker does not extend the sampled range (no false positive)"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE test_spoofed_marker (a UInt8, b String) ENGINE = Memory;
    INSERT INTO test_spoofed_marker FROM INFILE '${DATA_FILE_TSV}' FORMAT TSV;
" < /dev/null 2>&1 | check

echo "-- TSV: the failing row itself does not match the destination (explanation still fires)"
$CLICKHOUSE_LOCAL --query "
    CREATE TABLE test_spoofed_marker (a UInt8, b String) ENGINE = Memory;
    INSERT INTO test_spoofed_marker FROM INFILE '${DATA_FILE_TSV_MISMATCH}' FORMAT TSV;
" < /dev/null 2>&1 | check

echo "-- Values: the fake marker does not extend the sampled range, so the mismatch is still explained"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 0 --query "
    CREATE TABLE test_spoofed_marker_values (a Date, b Array(UInt8)) ENGINE = Memory;
    INSERT INTO test_spoofed_marker_values FROM INFILE '${DATA_FILE_VALUES}' FORMAT Values;
" < /dev/null 2>&1 | check

rm -f "$DATA_FILE_TSV" "$DATA_FILE_TSV_MISMATCH" "$DATA_FILE_VALUES"
