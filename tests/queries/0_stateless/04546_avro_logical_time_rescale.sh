#!/usr/bin/env bash
# Tags: no-fasttest
# Avro logical time-millis / time-micros must be rescaled into ClickHouse Time / Time64.
# A raw integer insert would turn noon (43200000 millis) into 43200000 seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/avro_logical_time.avro"

# The `avro` Python package is not available in CI, so the container file is encoded by hand.
python3 - "$DATA_FILE" <<'PY'
import json
import sys

def zigzag(n):
    n = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def avro_bytes(b):
    return zigzag(len(b)) + b

schema = json.dumps({
    "type": "record",
    "name": "Row",
    "fields": [
        {"name": "t_millis", "type": {"type": "int", "logicalType": "time-millis"}},
        {"name": "t_micros", "type": {"type": "long", "logicalType": "time-micros"}},
    ],
}).encode()

rows = [
    # 12:00:00
    (43200000, 43200000000),
    # 12:00:00.123000 / 12:00:00.123456
    (43200123, 43200123456),
]

sync = bytes(range(16))
data = b"".join(zigzag(millis) + zigzag(micros) for millis, micros in rows)
with open(sys.argv[1], "wb") as out:
    out.write(b"Obj\x01")
    out.write(zigzag(2))
    out.write(avro_bytes(b"avro.schema") + avro_bytes(schema))
    out.write(avro_bytes(b"avro.codec") + avro_bytes(b"null"))
    out.write(zigzag(0))
    out.write(sync)
    out.write(zigzag(len(rows)) + zigzag(len(data)) + data + sync)
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

# Downscale micros -> Time64(3) truncates, the same as `CAST` between `Time64` types.
cat "$DATA_FILE" | ${CLICKHOUSE_LOCAL} --input-format Avro --output-format TSV \
    -S "t_millis Time64(3), t_micros Time64(3)" -q 'SELECT * FROM table ORDER BY ALL'

rm -f "$DATA_FILE"
