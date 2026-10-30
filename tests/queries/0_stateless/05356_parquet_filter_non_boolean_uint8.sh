#!/usr/bin/env bash
# Tags: no-fasttest

# A PREWHERE filter on a UInt8 column holding values greater than 1 is passed as is to the Parquet
# page decoders of the columns read after it. Covers every encoding whose decoder counts the passing
# values of such a filter. The files have no page index and are read in many small row subgroups,
# which is what sends a filter to the decoders. Each result is compared with an unfiltered read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P="${CLICKHOUSE_DATABASE}"
trap 'rm -f "${USER_FILES_PATH:?}/${P}_"*.parquet' EXIT

python3 - "$USER_FILES_PATH/${P}" <<'PYEOF'
import sys, json
import pyarrow as pa
import pyarrow.parquet as pq

n = 3000
r = range(n)
cols = {
    "x": (pa.uint8(), [i % 4 for i in r], "PLAIN"),
    "b": (pa.bool_(), [i % 3 == 0 for i in r], "PLAIN"),
    "dbp64": (pa.int64(), [i * 7 for i in r], "DELTA_BINARY_PACKED"),
    "dbp32": (pa.int32(), [i * 3 for i in r], "DELTA_BINARY_PACKED"),
    "dbp16": (pa.int16(), [i * 5 for i in r], "DELTA_BINARY_PACKED"),
    "dbaf": (pa.binary(8), [b"%08d" % i for i in r], "DELTA_BYTE_ARRAY"),
    "bssf": (pa.float64(), [i * 0.5 for i in r], "BYTE_STREAM_SPLIT"),
    "bssi": (pa.int32(), [i * 11 for i in r], "BYTE_STREAM_SPLIT"),
    "ps": (pa.string(), ["p%d" % i for i in r], "PLAIN"),
    "dlba": (pa.string(), ["s%d" % i for i in r], "DELTA_LENGTH_BYTE_ARRAY"),
    "dba": (pa.string(), ["prefix%06d" % i for i in r], "DELTA_BYTE_ARRAY"),
    "g_plain": (pa.string(), ["POINT (%d %d)" % (i, i + 1) for i in r], "PLAIN"),
    "g_dlba": (pa.string(), ["POINT (%d %d)" % (i, i + 2) for i in r], "DELTA_LENGTH_BYTE_ARRAY"),
    "g_dba": (pa.string(), ["POINT (%d %d)" % (i, i + 3) for i in r], "DELTA_BYTE_ARRAY"),
}
# GeoParquet WKT columns are read through a non-trivial converter.
geo = {"version": "1.1.0", "primary_column": "g_plain",
       "columns": {c: {"encoding": "WKT", "geometry_types": ["Point"]} for c in cols if c.startswith("g_")}}
schema = pa.schema([pa.field(c, t, nullable=False) for c, (t, _, _) in cols.items()],
                   metadata={b"geo": json.dumps(geo).encode()})
table = pa.Table.from_arrays([pa.array(v, type=t) for t, v, _ in cols.values()], schema=schema)
pq.write_table(table, sys.argv[1] + "_pa.parquet", column_encoding={c: e for c, (_, _, e) in cols.items()},
               use_dictionary=False, write_page_index=False, data_page_version="1.0", compression="none")
PYEOF

S="SETTINGS input_format_parquet_max_block_size = 100, input_format_parquet_prefer_block_bytes = 0, use_query_condition_cache = 0"
STRUCTURE="x UInt8, b Bool, dbp64 Int64, dbp32 Int32, dbp16 Int16, dbaf FixedString(8), bssf Float64, bssi Int32, ps String, dlba String, dba String, g_plain Point, g_dlba Point, g_dba Point"

for c in b dbp64 dbp32 dbp16 dbaf bssf bssi ps dlba dba g_plain g_dlba g_dba; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '$c', count(), sum(cityHash64($c)) AS h,
            (count(), h) = (SELECT countIf(x != 0), sumIf(cityHash64($c), x != 0) FROM file('${P}_pa.parquet', Parquet, '$STRUCTURE'))
        FROM file('${P}_pa.parquet', Parquet, '$STRUCTURE') PREWHERE x $S"
done

echo "--- ClickHouse writer ---"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${P}_ch.parquet', Parquet)
    SELECT toUInt8(number % 4) AS x, (number % 3 = 0)::Bool AS b FROM numbers(3000)
    SETTINGS output_format_parquet_write_page_index = 0"
${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(b) FROM file('${P}_ch.parquet', Parquet, 'x UInt8, b Bool') PREWHERE x $S"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf(b) FROM file('${P}_ch.parquet', Parquet, 'x UInt8, b Bool') WHERE x
    $S, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

echo "--- BYTE_ARRAY Decimal, DELTA_BYTE_ARRAY ---"
${CLICKHOUSE_LOCAL} --query "
    SELECT value FROM file('$CUR_DIR/data_parquet/05035_delta_byte_array_decimal.parquet')
    PREWHERE toUInt8(keep * 3)
    ORDER BY value
    SETTINGS input_format_parquet_max_block_size = 1, input_format_parquet_prefer_block_bytes = 0, use_query_condition_cache = 0"
