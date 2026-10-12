#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# With `input_format_values_deduce_templates_of_expressions` (the default), `ValuesBlockInputFormat`
# reads the whole block before evaluating the deduced templates, so a parse error thrown while
# evaluating them happens when the parser has reached exactly the rows of that block - not one row
# more. Sampling one extra row changes the inferred structure of the data and thus the diagnostic.
#
# `max_insert_block_size` is 2, so the first block holds the first two rows and the third row must
# not be sampled: it is the only row where the second column holds a number, so sampling it would
# make the inference of that column contradictory and lose the explanation of the parse error.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- the first unread row of the next block is not sampled, so the mismatch is still explained"
$CLICKHOUSE_LOCAL --max_insert_block_size 2 --query "
    CREATE TABLE test_values_template_block (a Date, b Int64) ENGINE = Memory;
    INSERT INTO test_values_template_block VALUES ('abcd', 'text'), ('2020-01-01', 'text'), ('2020-01-01', 1);
" < /dev/null 2>&1 | check

echo "-- a value error where the structure matches is not explained as a structure mismatch"
$CLICKHOUSE_LOCAL --max_insert_block_size 2 --query "
    CREATE TABLE test_values_template_block (a Date, b Int64) ENGINE = Memory;
    INSERT INTO test_values_template_block VALUES ('abcd', 1), ('2020-01-01', 1), ('2020-01-01', 1);
" < /dev/null 2>&1 | check
