#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Native` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# The formats that cast a decoded source `String` column to the destination type (`Native` and the
# columnar formats) parse the string with the destination's own whole-text semantics, so the
# diagnostic must validate the sampled values against the concrete destination type, not against a
# generic "is it a number" check: `-1` is numeric text, but the cast into a `UInt8` column still
# fails with `CANNOT_PARSE_NUMBER`, a genuine structure mismatch. The mirror image is a `Nullable`
# destination: `castColumn` of a `String` source column into a `Nullable` type deliberately turns
# unparsable text into `NULL` instead of throwing, so such a column must not turn an unrelated parse
# error in a sibling column into a bogus structure mismatch.
#
# The `UUID` column comes first because `Native` casts the columns in order: an invalid `UUID` fails
# with `CANNOT_PARSE_UUID`, a genuine parse error, which is what triggers the diagnostic.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

insert() {
    local structure="$1"
    local data_file="$2"
    {
        echo "CREATE TABLE t ($structure) ENGINE = Memory; INSERT INTO t FORMAT Native"
        cat "$data_file"
    } | $CLICKHOUSE_LOCAL 2>&1 | check
}

data_negative="$CLICKHOUSE_TMP/data_05046_negative.native"
data_text="$CLICKHOUSE_TMP/data_05046_text.native"
data_not_bool="$CLICKHOUSE_TMP/data_05046_not_bool.native"
data_null_word="$CLICKHOUSE_TMP/data_05046_null_word.native"
$CLICKHOUSE_LOCAL -q "SELECT '-1' AS x FORMAT Native" > "$data_negative"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'abc' AS x FORMAT Native" > "$data_text"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'maybe' AS b FORMAT Native" > "$data_not_bool"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'NULL' AS b FORMAT Native" > "$data_null_word"

echo "-- Native, numeric text that the destination type cannot represent is a mismatch"
insert "x UInt8" "$data_negative"

echo "-- Native, the same numeric text is valid for a signed destination (clean insert)"
insert "x Int8" "$data_negative"

echo "-- Native, non-numeric text for a Nullable numeric column casts to NULL and is not a mismatch"
insert "u UUID, x Nullable(UInt8)" "$data_text"

echo "-- Native, non-numeric text for a plain numeric column stays a mismatch"
insert "u UUID, x UInt8" "$data_text"

echo "-- Native, text that is not a valid Bool is a mismatch also for a Nullable(Bool) column"
insert "u UUID, b Nullable(Bool)" "$data_not_bool"

echo "-- Native, the NULL literal is valid text for a Nullable(Bool) column"
insert "u UUID, b Nullable(Bool)" "$data_null_word"

rm -f "$data_negative" "$data_text" "$data_not_bool" "$data_null_word"
