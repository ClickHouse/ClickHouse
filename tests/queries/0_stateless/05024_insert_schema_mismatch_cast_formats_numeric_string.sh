#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Native` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# The formats that cast a decoded source `String` column to the destination type (`Native` and the
# columnar formats) accept a numeric string such as `1` into a numeric column, so a valid sibling
# column must not turn an unrelated parse error into a structure mismatch. Only a value that is not
# numeric text is a genuine "text where a number is expected" mismatch. Schema inference alone cannot
# tell the two apart here — the source column is typed `String` in both cases, and the second
# inference pass with number-from-string inference enabled reads back the very same typed column — so
# the diagnostic looks at the sampled values instead.
#
# The `UUID` column comes first because `Native` casts the columns in order: an invalid `UUID` fails
# with `CANNOT_PARSE_UUID`, a genuine parse error, which is what triggers the diagnostic. A failing
# cast into a numeric column raises `CANNOT_PARSE_TEXT`, which is not classified as a parse error, so
# it would stop the insert before any diagnostic is attached.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

data_numeric="$CLICKHOUSE_TMP/data_05024_numeric.native"
data_text="$CLICKHOUSE_TMP/data_05024_text.native"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, '1' AS x FORMAT Native" > "$data_numeric"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'not_a_number' AS x FORMAT Native" > "$data_text"

echo "-- Native, a numeric string for a numeric column is valid (no false positive)"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t FORMAT Native"
    cat "$data_numeric"
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- Native, genuinely non-numeric text for a numeric column is a mismatch"
{
    echo "CREATE TABLE t (u UUID, x UInt8) ENGINE = Memory; INSERT INTO t FORMAT Native"
    cat "$data_text"
} | $CLICKHOUSE_LOCAL 2>&1 | check

rm -f "$data_numeric" "$data_text"
