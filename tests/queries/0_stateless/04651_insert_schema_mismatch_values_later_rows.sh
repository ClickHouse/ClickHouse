#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The `Values` format is not an `IRowInputFormat`, but it is row-aware, so it also has to report the
# row the parser had reached when a parse error was thrown: the diagnostic must be derived from the
# rows the parser actually read, not from rows it never got to.
#
# `input_format_values_deduce_templates_of_expressions` is turned off so that the expressions are
# evaluated row by row; with templates the whole block is read before the expressions are evaluated,
# so the parser really has reached the later rows by then.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- the failing row itself mismatches; a later row does not hide the explanation"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 0 --query "
    CREATE TABLE test_values_row_bound (a Date, b Array(UInt8)) ENGINE = Memory;
    INSERT INTO test_values_row_bound VALUES ('abcd', 'text'), ('2020-01-01', [1]);
" < /dev/null 2>&1 | check

echo "-- the failing row matches the expected structure; a later row does not create a false positive"
$CLICKHOUSE_LOCAL --input_format_values_deduce_templates_of_expressions 0 --query "
    CREATE TABLE test_values_row_bound (a Date, b Array(UInt8)) ENGINE = Memory;
    INSERT INTO test_values_row_bound VALUES ('abcd', [1]), ('2020-01-01', 'text');
" < /dev/null 2>&1 | check
