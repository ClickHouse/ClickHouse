#!/usr/bin/env bash
# Tags: no-fasttest
# Avro logical time-millis / time-micros must be rescaled into ClickHouse Time / Time64.
# A raw integer insert would turn noon (43200000 millis) into 43200000 seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/avro_logical_time.avro"

python3 - "$DATA_FILE" <<'PY'
import datetime
import sys
from avro import schema, datafile, io

path = sys.argv[1]
schema_json = """{
  "type": "record",
  "name": "Row",
  "fields": [
    {"name": "t_millis", "type": {"type": "int", "logicalType": "time-millis"}},
    {"name": "t_micros", "type": {"type": "long", "logicalType": "time-micros"}}
  ]
}"""
parsed = schema.parse(schema_json)
with open(path, "wb") as out:
    writer = datafile.DataFileWriter(out, io.DatumWriter(), parsed)
    # 12:00:00
    writer.append({"t_millis": datetime.time(12, 0, 0), "t_micros": datetime.time(12, 0, 0)})
    # 12:00:00.123000 / 12:00:00.123456
    writer.append({"t_millis": datetime.time(12, 0, 0, 123000), "t_micros": datetime.time(12, 0, 0, 123456)})
    writer.close()
PY

# Matching Time64 scales: raw ticks already match physical Avro units.
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format TSV \
    -S "t_millis Time64(3), t_micros Time64(6)" -q 'SELECT * FROM table ORDER BY ALL'

# Rescale into Time (seconds).
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format TSV \
    -S "t_millis Time, t_micros Time" -q 'SELECT * FROM table ORDER BY ALL'

# Time64(0) is also second precision: truncate like Time.
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format TSV \
    -S "t_millis Time64(0), t_micros Time64(0)" -q 'SELECT * FROM table ORDER BY ALL'

# Upscale millis -> Time64(6).
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format TSV \
    -S "t_millis Time64(6), t_micros Time64(6)" -q 'SELECT * FROM table ORDER BY ALL'

# Downscale micros -> Time64(3) must be rejected (would lose precision).
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro \
    -S "t_millis Time64(3), t_micros Time64(3)" -q 'SELECT t_micros FROM table' 2>&1 \
    | grep -o 'Cannot insert Avro time'

rm -f "$DATA_FILE"
