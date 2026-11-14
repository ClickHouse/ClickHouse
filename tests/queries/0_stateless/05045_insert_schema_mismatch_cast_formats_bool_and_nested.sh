#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Native` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/pull/110626
# The formats that cast a decoded source `String` column to the destination type (`Native` and the
# columnar formats) read the value with the whole-text deserializer of the destination, so a source
# string such as `true` is valid for a `Bool` column and `[1,2]` is valid for an `Array(UInt8)` one.
# Schema inference only ever reports `String` for such a column, so the sampled values decide whether
# it is a genuine mismatch — a valid sibling column must not turn an unrelated parse error into one.
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

data_bool="$CLICKHOUSE_TMP/data_05045_bool.native"
data_not_bool="$CLICKHOUSE_TMP/data_05045_not_bool.native"
data_array="$CLICKHOUSE_TMP/data_05045_array.native"
data_not_array="$CLICKHOUSE_TMP/data_05045_not_array.native"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'true' AS b FORMAT Native" > "$data_bool"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'maybe' AS b FORMAT Native" > "$data_not_bool"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, '[1,2]' AS a FORMAT Native" > "$data_array"
$CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 'abc' AS a FORMAT Native" > "$data_not_array"

echo "-- Native, a valid Bool text for a Bool column is not a mismatch"
insert "u UUID, b Bool" "$data_bool"

echo "-- Native, text that is not a valid Bool is a mismatch"
insert "u UUID, b Bool" "$data_not_bool"

echo "-- Native, a valid Array text for an Array column is not a mismatch"
insert "u UUID, a Array(UInt8)" "$data_array"

echo "-- Native, text that is not a valid Array is a mismatch"
insert "u UUID, a Array(UInt8)" "$data_not_array"

rm -f "$data_bool" "$data_not_bool" "$data_array" "$data_not_array"
