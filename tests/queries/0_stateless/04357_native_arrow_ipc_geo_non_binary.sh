#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for the native Arrow IPC reader: the schema-level "geo" metadata is untrusted, so a
# file may tag a non-binary field (here an Int32) as a WKB geometry column. The reader must reject it
# with a clean INCORRECT_DATA instead of a bad-cast exception (checked builds) or undefined behavior
# (release), which is what `assert_cast<ColumnString>` would have caused before the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_geo_int.arrow"
trap 'rm -f "$DATA_FILE"' EXIT

# Build a valid Arrow IPC file whose column "x" is Int32, but attach schema-level "geo" metadata that
# (falsely) declares "x" to be a WKB geometry column.
python3 - "$DATA_FILE" <<'EOF'
import sys
import json
import pyarrow as pa
import pyarrow.ipc as ipc

path = sys.argv[1]

arr = pa.array([1, 2, 3], type=pa.int32())
tbl = pa.Table.from_arrays([arr], names=['x'])
geo_meta = json.dumps({'columns': {'x': {'encoding': 'WKB', 'geometry_types': ['Point']}}})

meta = tbl.schema.metadata or {}
meta[b'geo'] = geo_meta.encode()
tbl = tbl.replace_schema_metadata(meta)

with pa.OSFile(path, 'wb') as f:
    w = ipc.new_file(f, tbl.schema)
    w.write_table(tbl)
    w.close()
EOF

$CLICKHOUSE_LOCAL --input_format_arrow_use_native_reader=1 --input_format_parquet_allow_geoparquet_parser=1 \
    --query "SELECT x FROM file('${DATA_FILE}', 'Arrow')" 2>&1 \
    | grep -oF 'INCORRECT_DATA' || echo 'FAIL: expected INCORRECT_DATA'
