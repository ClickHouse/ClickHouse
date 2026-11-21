#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not built in the fast test (contrib avrocpp)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# The Avro parser skips input fields that have no matching destination column unconditionally
# (`AvroDeserializer::createAction` builds a skip action for them), without consulting
# `input_format_skip_unknown_fields`. So a field present in the data but unknown to the destination
# is not a structure mismatch even when that setting is disabled, and a genuine parse error in a
# known field must not pick up a misleading "structure mismatch" explanation because of it.
#
# The parse error is crafted by patching the enum index of the value of `n` to an out-of-range
# value, which the parser rejects with `Avro enum index 3 is out of range` (`INCORRECT_DATA`),
# while schema inference (which reads only the schema from the file header) is unaffected.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

AVRO_FILE="${CLICKHOUSE_TMP}/04635_data.avro"

$CLICKHOUSE_LOCAL --output_format_avro_codec 'null' --query \
    "SELECT CAST('a', 'Enum8(\'a\' = 1)') AS n, 'x' AS extra FORMAT Avro" > "$AVRO_FILE"

# Patch the enum index of the value of `n` in the single data block (`block-count(1) block-size(3)
# enum-index(0) string-length(1) 'x'` = 02 06 00 02 78) to the out-of-range index 3 (06).
python3 -c "
import sys
path = sys.argv[1]
data = open(path, 'rb').read()
idx = data.rfind(b'\x02\x06\x00\x02x')
assert idx > 0, 'record bytes not found'
open(path, 'wb').write(data[:idx + 2] + b'\x06' + data[idx + 3:])
" "$AVRO_FILE"

echo "-- Avro, extra unknown field plus a bad value in a known field, input_format_skip_unknown_fields = 0 (no false positive)"
$CLICKHOUSE_LOCAL --input_format_skip_unknown_fields 0 --query \
    "CREATE TABLE t (n String) ENGINE = Memory; INSERT INTO t FORMAT Avro" < "$AVRO_FILE" 2>&1 | check

echo "-- Avro, the same with the default input_format_skip_unknown_fields = 1 (no false positive)"
$CLICKHOUSE_LOCAL --input_format_skip_unknown_fields 1 --query \
    "CREATE TABLE t (n String) ENGINE = Memory; INSERT INTO t FORMAT Avro" < "$AVRO_FILE" 2>&1 | check

rm -f "$AVRO_FILE"
