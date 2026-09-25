#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must infer the structure only from the rows the parser had
# reached, including the row whose parsing failed — not from the whole payload. Otherwise a later
# row the parser never got to could widen the inferred type of a column (e.g. to `String`) and turn
# a value-level parse error in an earlier row into a bogus "structure mismatch" explanation.
#
# `input_format_parallel_parsing` is pinned to 0 to exercise the serial path deterministically;
# the parallel path (where the failing child parser's row count is propagated through
# `ParallelParsingInputFormat`) is covered by 04761_insert_schema_mismatch_parallel_parsing_row_bound.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- TSV, the first row fails on a value; a later conflicting row is not sampled (no false positive)"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n2\ttext\n' \
    | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 2>&1 | check

echo "-- TSV, the failing row itself is sampled: text where a number is expected (explanation still fires)"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\ntext\t1\n' \
    | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 2>&1 | check

echo "-- TSV, the second row fails and is sampled together with the first one (explanation still fires)"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t2\n2\ttext\n' \
    | $CLICKHOUSE_LOCAL --input_format_parallel_parsing 0 2>&1 | check
