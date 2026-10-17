#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Arrow` format is not available in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The formats that cast a typed source column to the destination type (`Native`, `Parquet`, `Arrow`)
# accept only an unsigned integer column for an `IPv4` destination: `CAST` from a signed integer or a
# floating-point type into `IPv4` is not implemented. `ORC` accepts only its `int` (`Int32`) column
# there. So when a sibling column fails to parse, a signed `Int64` source column going into an `IPv4`
# column is a genuine structure mismatch and must be explained, while an unsigned one is not.
#
# The column `u` comes first and holds a string that is not a valid `UUID`, which fails with a
# genuine parse error (`CANNOT_PARSE_UUID`) and triggers the diagnostic. `Native` and `Arrow` convert
# the columns in order, so the `u` error always comes first (`Parquet` decodes the columns in
# parallel, so which of the two errors is reported is not deterministic there).

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA=$CLICKHOUSE_TMP/data_05331

for format in Native Arrow; do
    for type in Int64 Float64 UInt32; do
        echo "-- $format: $type source column into an IPv4 column"
        $CLICKHOUSE_LOCAL -q "SELECT 'not-a-uuid' AS u, 16909060::$type AS ip FORMAT $format" > "$DATA"
        {
            echo "CREATE TABLE t (u UUID, ip IPv4) ENGINE = Memory; INSERT INTO t FORMAT $format"
            cat "$DATA"
        } | $CLICKHOUSE_LOCAL 2>&1 | check
    done
done

rm -f "$DATA"
