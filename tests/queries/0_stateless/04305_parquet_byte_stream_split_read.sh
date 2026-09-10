#!/usr/bin/env bash
# Tags: no-fasttest

# Reading Parquet FLOAT/DOUBLE columns encoded with BYTE_STREAM_SPLIT. ClickHouse's writer never emits this
# encoding, so the decoder (which uses Arrow's SIMD ByteStreamSplitDecode) is only reached via externally
# produced files; here they are generated with pyarrow at test time. Covers plain, nullable (definition
# levels) and a sub-SIMD-batch tail size. Checks are order-independent so they are stable under random
# settings, and the files are prefixed with the (unique) database name so the test is safe to run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_DATABASE}"
trap 'rm -f "${USER_FILES_PATH:?}/${P}_"*.parquet' EXIT

python3 - "$USER_FILES_PATH/${P}" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

prefix = sys.argv[1]

def w(name, table, enc):
    pq.write_table(table, prefix + name, column_encoding=enc, use_dictionary=False,
                   data_page_version="2.0", compression="none")

n = 10000
a = pa.array([i * 1.25 - 7.0 for i in range(n)], type=pa.float32())
b = pa.array([i * 0.0009765625 + 1e6 for i in range(n)], type=pa.float64())
w("_f32_f64.parquet", pa.table({"a": a, "b": b}),
  {"a": "BYTE_STREAM_SPLIT", "b": "BYTE_STREAM_SPLIT"})

m = 8000
av = [None if i % 4 == 0 else i * 2.5 for i in range(m)]
bv = [None if i % 3 == 0 else i * 7.5 - 3.0 for i in range(m)]
w("_nullable.parquet", pa.table({
      "a": pa.array(av, type=pa.float32()), "b": pa.array(bv, type=pa.float64()),
      "k": pa.array(list(range(m)), type=pa.int32())}),
  {"a": "BYTE_STREAM_SPLIT", "b": "BYTE_STREAM_SPLIT"})

w("_tail.parquet", pa.table({"d": pa.array([i * 1.5 for i in range(37)], type=pa.float64())}),
  {"d": "BYTE_STREAM_SPLIT"})
PYEOF

# Structures are specified explicitly so the result is independent of schema-inference settings.
echo "--- f32/f64, plain ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(a)), sum(cityHash64(b)) FROM file('${P}_f32_f64.parquet', 'Parquet', 'a Float32, b Float64')"
${CLICKHOUSE_CLIENT} --query "SELECT a, b FROM file('${P}_f32_f64.parquet', 'Parquet', 'a Float32, b Float64') ORDER BY b LIMIT 3"

echo "--- f32/f64, nullable (definition levels) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), count(a), count(b), sum(cityHash64(a)), sum(cityHash64(b)) FROM file('${P}_nullable.parquet', 'Parquet', 'a Nullable(Float32), b Nullable(Float64), k Int32')"
${CLICKHOUSE_CLIENT} --query "SELECT k, a, b FROM file('${P}_nullable.parquet', 'Parquet', 'a Nullable(Float32), b Nullable(Float64), k Int32') ORDER BY k LIMIT 6"

echo "--- tail size (37 values) ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(d)) FROM file('${P}_tail.parquet', 'Parquet', 'd Float64')"
${CLICKHOUSE_CLIENT} --query "SELECT d FROM file('${P}_tail.parquet', 'Parquet', 'd Float64') ORDER BY d"
