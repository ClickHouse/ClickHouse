#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Native` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# `Native` casts a source `String` column to the destination type through the destination's full
# serialization, so for a `Nullable(Tuple(...))` destination the `NULL` literal is valid text. The
# diagnostic must not blame such a column when the parse error is in a sibling column (the invalid
# `UUID`, which `Native` casts first).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

insert() {
    local structure="$1"
    local data_file="$2"
    $CLICKHOUSE_LOCAL --query "CREATE TABLE t ($structure) ENGINE = Memory; INSERT INTO t FORMAT Native" < "$data_file" 2>&1 | check
}

data_null_word="$CLICKHOUSE_TMP/data_05318_null_word.native"
data_text="$CLICKHOUSE_TMP/data_05318_text.native"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'NULL' AS a FORMAT Native" > "$data_null_word"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'abc' AS a FORMAT Native" > "$data_text"

echo "-- Native, the NULL literal is valid text for a Nullable(Tuple) column"
insert "u UUID, a Nullable(Tuple(UInt8, UInt8))" "$data_null_word"

echo "-- Native, text that is not a valid Tuple is a mismatch also for a Nullable(Tuple) column"
insert "u UUID, a Nullable(Tuple(UInt8, UInt8))" "$data_text"

rm -f "$data_null_word" "$data_text"
