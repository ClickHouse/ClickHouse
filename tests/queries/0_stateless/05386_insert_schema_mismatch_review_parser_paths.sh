#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Parser paths the structure-mismatch explanation must mirror. In every case a sibling column has the
# real (value-level) parse error, so the explanation must appear only when the checked column itself
# does not fit.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

run() {
    printf '%s' "$1" | $CLICKHOUSE_LOCAL $4 --query "CREATE TABLE t ($2) ENGINE = Memory; INSERT INTO t FORMAT $3" 2>&1 | check
}

echo "-- JSONEachRow: a homogeneous array read positionally into a Tuple(Bool, UInt8)"
run '{"t":[1,2],"n":1.5}' 't Tuple(Bool, UInt8), n UInt8' JSONEachRow
run '{"t":[2,1],"n":1.5}' 't Tuple(Bool, UInt8), n UInt8' JSONEachRow

echo "-- JSONEachRow: homogeneous pairs read into a Map with input_format_json_map_as_array_of_tuples"
run '{"m":[[1,2],[3,4]],"n":1.5}' 'm Map(UInt8, UInt8), n UInt8' JSONEachRow --input_format_json_map_as_array_of_tuples=1
run '{"m":[[1,2,3]],"n":1.5}' 'm Map(UInt8, UInt8), n UInt8' JSONEachRow --input_format_json_map_as_array_of_tuples=1
run '{"m":[["x","y"]],"n":1.5}' 'm Map(UInt8, UInt8), n UInt8' JSONEachRow --input_format_json_map_as_array_of_tuples=1

echo "-- Form: a numeric value read with the whole-text deserializer"
run 'a=1&b=x' 'a UInt8, b DateTime' Form --input_format_tsv_use_best_effort_in_schema_inference=0
run 'a=y&b=x' 'a UInt8, b DateTime' Form --input_format_tsv_use_best_effort_in_schema_inference=0

echo "-- Values: a quoted string literal read into a nested column with the whole-text deserializer"
run "('[1,2]', 'x')" 'a Array(UInt8), b DateTime' Values
run "('oops', 'x')" 'a Array(UInt8), b DateTime' Values

echo "-- BSONEachRow: an ObjectId read into a String column"
# The field `u` is a BSON binary with the UUID subtype but only 4 bytes of payload (the real parse error).
{
    echo "CREATE TABLE t (u UUID, s String) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), s: ObjectId('ABCDEFGHIJKL')}
    printf '\x20\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x07s\x00ABCDEFGHIJKL\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check
