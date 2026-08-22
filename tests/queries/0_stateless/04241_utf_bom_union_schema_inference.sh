#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "$CLICKHOUSE_TEST_UNIQUE_NAME"

# Write data1.jsonl (UTF-8)
echo '{"obj" : {"f1" : 1, "f2" : "2020-01-01"}}' > "$CLICKHOUSE_TEST_UNIQUE_NAME/data1.jsonl"

# Write data2.jsonl (UTF-16LE with BOM)
python3 -c 'import sys; open("'"$CLICKHOUSE_TEST_UNIQUE_NAME"'/data2.jsonl", "wb").write(b"\xff\xfe" + "{\"obj\" : {\"f3\" : 2, \"f2\" : \"Some string\"}}\n".encode("utf-16le"))'

# Schema inference (union mode)
$CLICKHOUSE_LOCAL -q "desc file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2}.jsonl') settings schema_inference_mode='union'"

# Read data (union mode)
$CLICKHOUSE_LOCAL -q "select obj.f1, obj.f2, obj.f3 from file('$CLICKHOUSE_TEST_UNIQUE_NAME/data{1,2}.jsonl') order by obj.f2 settings schema_inference_mode='union'"

# Alias test: NDJSON format with UTF-16LE BOM
python3 -c 'open("'"$CLICKHOUSE_TEST_UNIQUE_NAME"'/data3.ndjson", "wb").write(b"\xff\xfe" + "{\"obj\" : {\"f4\" : 4}}\n".encode("utf-16le"))'

# Read from NDJSON file with BOM
$CLICKHOUSE_LOCAL -q "select obj.f4 from file('$CLICKHOUSE_TEST_UNIQUE_NAME/data3.ndjson', 'NDJSON')"

rm -rf "$CLICKHOUSE_TEST_UNIQUE_NAME"
