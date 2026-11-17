#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR=$CUR_DIR/data_avro
cat "$DATA_DIR"/nested_complex_incorrect_data.avro | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format CSV -S "\"b.b2_null_str\" Nullable(String)"    -q 'select * from table;' 2>&1 | grep -i 'index out of boundary' -o

# An out-of-range union index read into Variant, Nullable, with input_format_null_as_default, and from a one-branch union.
# u: ["null", "int", "string"], one row "x" with union index 10.
VARIANT='T2JqAQQWYXZyby5zY2hlbWG+AXsidHlwZSI6ICJyZWNvcmQiLCAibmFtZSI6ICJyb3ciLCAiZmllbGRzIjogW3sibmFtZSI6ICJ1IiwgInR5cGUiOiBbIm51bGwiLCAiaW50IiwgInN0cmluZyJdfV19FGF2cm8uY29kZWMIbnVsbAAAAQIDBAUGBwgJCgsMDQ4PAgYUAngAAQIDBAUGBwgJCgsMDQ4P'
# u: ["null", "string"], n: int, rows ("x", 7) with union index 10 and ("yz", 8).
NULLABLE='T2JqAQQWYXZyby5zY2hlbWHsAXsidHlwZSI6ICJyZWNvcmQiLCAibmFtZSI6ICJyb3ciLCAiZmllbGRzIjogW3sibmFtZSI6ICJ1IiwgInR5cGUiOiBbIm51bGwiLCAic3RyaW5nIl19LCB7Im5hbWUiOiAibiIsICJ0eXBlIjogImludCJ9XX0UYXZyby5jb2RlYwhudWxsAAABAgMEBQYHCAkKCwwNDg8EEhQCeA4CBHl6EAABAgMEBQYHCAkKCwwNDg8='
# u: ["string"], one row "x" with union index 1.
SINGLE='T2JqAQQWYXZyby5zY2hlbWGgAXsidHlwZSI6ICJyZWNvcmQiLCAibmFtZSI6ICJyb3ciLCAiZmllbGRzIjogW3sibmFtZSI6ICJ1IiwgInR5cGUiOiBbInN0cmluZyJdfV19FGF2cm8uY29kZWMIbnVsbAAAAQIDBAUGBwgJCgsMDQ4PAgYCAngAAQIDBAUGBwgJCgsMDQ4P'

${CLICKHOUSE_LOCAL} -q "SELECT * FROM format(Avro, base64Decode('$VARIANT'))" 2>&1 | grep -o -E 'Union index out of boundary: got [0-9]+, the number of branches is [0-9]+|INCORRECT_DATA' | head -n 2
${CLICKHOUSE_LOCAL} -q "SELECT * FROM format(Avro, base64Decode('$NULLABLE'))" 2>&1 | grep -o -E 'Union index out of boundary: got [0-9]+, the number of branches is [0-9]+|INCORRECT_DATA' | head -n 2
${CLICKHOUSE_LOCAL} -q "SELECT * FROM format(Avro, 'u String, n Int32', base64Decode('$NULLABLE')) SETTINGS input_format_null_as_default = 1" 2>&1 | grep -o -E 'Union index out of boundary: got [0-9]+, the number of branches is [0-9]+|INCORRECT_DATA' | head -n 2
${CLICKHOUSE_LOCAL} -q "SELECT * FROM format(Avro, base64Decode('$SINGLE'))" 2>&1 | grep -o -E 'Union index out of boundary: got [0-9]+, the number of branches is [0-9]+|INCORRECT_DATA' | head -n 2
