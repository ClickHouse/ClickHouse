#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# `INSERT ... FROM INFILE` takes the row bound for schema inference from the parse error message, so
# it has to understand the `Values` form of that message (` at row N`, counting the rows that were
# parsed completely) as well as the `(at row N)` form of `IRowInputFormat`. Otherwise rows the parser
# never reached are sampled as well, which changes the inferred structure of the data: in the data
# below, inference over both rows is contradictory for the second column, so the explanation of the
# parse error in the first row would be lost.
#
# `input_format_values_deduce_templates_of_expressions` is turned off so that the expressions are
# evaluated row by row and the failing row is reported in the message.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/04657_infile_values_${CLICKHOUSE_DATABASE}.values"
printf "('abcd', 1), ('2020-01-01', 'text')\n" > "$DATA_FILE"

echo "-- only the failing row is sampled, so its mismatch with the destination is explained"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 0 --query "
    CREATE TABLE test_infile_values (a Date, b Array(UInt8)) ENGINE = Memory;
    INSERT INTO test_infile_values FROM INFILE '${DATA_FILE}' FORMAT Values;
" < /dev/null 2>&1 | check

echo "-- the failing row matches the expected structure; the second row is not sampled (no false positive)"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 0 --query "
    CREATE TABLE test_infile_values (a Date, b Int64) ENGINE = Memory;
    INSERT INTO test_infile_values FROM INFILE '${DATA_FILE}' FORMAT Values;
" < /dev/null 2>&1 | check

rm -f "$DATA_FILE"
