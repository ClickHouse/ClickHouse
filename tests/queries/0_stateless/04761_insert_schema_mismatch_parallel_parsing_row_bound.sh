#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The schema-mismatch diagnostic must infer the structure only from the rows the parser had
# reached, including the row whose parsing failed — and that must hold under
# `ParallelParsingInputFormat` too. The outer format propagates the failing child parser's row
# count (the child counts rows globally, seeded with its unit's offset), so a later row the
# parser never got to cannot widen the inferred type of a column (e.g. to `String`) and turn a
# value-level parse error in an earlier row into a bogus "structure mismatch" explanation.
#
# `min_chunk_bytes_for_parallel_parsing` is tiny so the payload spans many parser units and the
# segmenting thread reads well ahead of the failing parser.

PHRASE="does not match the structure expected by the query"

SETTINGS="--input_format_parallel_parsing 1 --max_threads 4 --min_chunk_bytes_for_parallel_parsing 64"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE=$CLICKHOUSE_TMP/data_04761.tsv

echo "-- the first row fails on a value; a later conflicting row is not sampled (no false positive)"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\n1\t1.5\n2\ttext\n' \
    | $CLICKHOUSE_LOCAL $SETTINGS 2>&1 | check

echo "-- the failing row itself is sampled: text where a number is expected (explanation still fires)"
printf 'CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV\ntext\t1\n' \
    | $CLICKHOUSE_LOCAL $SETTINGS 2>&1 | check

echo "-- the failing row is deep in a later parser unit; rows up to it are sampled (explanation still fires)"
{
    seq 1 5000 | awk '{print 1 "\t" 2}'
    printf 'text\t1\n'
} > "$DATA_FILE"
{
    echo "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV"
    cat "$DATA_FILE"
} | $CLICKHOUSE_LOCAL $SETTINGS 2>&1 | check

echo "-- the failing row is deep in a later parser unit; a conflicting row after it is not sampled (no false positive)"
{
    seq 1 5000 | awk '{print 1 "\t" 2}'
    printf '1\t1.5\n'
    printf '2\ttext\n'
} > "$DATA_FILE"
{
    echo "CREATE TABLE t (a UInt8, b UInt8) ENGINE = Memory; INSERT INTO t FORMAT TSV"
    cat "$DATA_FILE"
} | $CLICKHOUSE_LOCAL $SETTINGS 2>&1 | check

rm -f "$DATA_FILE"
