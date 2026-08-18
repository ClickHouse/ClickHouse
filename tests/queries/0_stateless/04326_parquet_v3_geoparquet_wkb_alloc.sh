#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs pyarrow to write the GeoParquet file.

# Regression test for an untracked-allocation DoS in the Parquet V3 GeoParquet (WKB) reader.
# The WKB reader took an element count straight off the wire and `reserve`d a std::vector of that
# many points before reading any coordinate bytes. The default container tracking is non-throwing,
# so a 9-byte WKB value claiming 100M points forced a ~1.6 GB allocation that bypassed
# max_memory_usage (the query failed later with CANNOT_READ_ALL_DATA, never MEMORY_LIMIT_EXCEEDED).
# The geometry containers now use AllocatorWithMemoryTracking, so the oversized reserve is charged
# to the memory tracker and rejected with MEMORY_LIMIT_EXCEEDED under a small limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

python3 - "$WORK_DIR" <<'PYEOF'
import sys, struct, json
import pyarrow as pa
import pyarrow.parquet as pq
work = sys.argv[1]
# WKB LineString header: little-endian, type=2 (LineString), num_points=100M, no coordinates.
wkb = struct.pack('<BII', 1, 2, 100000000)
t = pa.table({'g': pa.array([wkb], type=pa.binary())})
geo = json.dumps({"version": "1.0.0", "primary_column": "g",
                  "columns": {"g": {"encoding": "WKB", "geometry_types": ["LineString"]}}})
t = t.replace_schema_metadata({b'geo': geo.encode()})
pq.write_table(t, f"{work}/geo_bomb.parquet", compression='none', use_dictionary=False)
PYEOF

# The oversized reserve must be charged to the memory tracker and rejected under a 100 MB limit,
# instead of allocating ~1.6 GB outside the tracker.
out=$(${CLICKHOUSE_LOCAL} --query "
    SELECT g FROM file('${WORK_DIR}/geo_bomb.parquet', Parquet) FORMAT Null
    SETTINGS input_format_parquet_use_native_reader_v3 = 1,
             input_format_parquet_allow_geoparquet_parser = 1,
             max_memory_usage = 104857600" 2>&1)
if echo "$out" | grep -q "MEMORY_LIMIT_EXCEEDED"; then
    echo "geo_bomb: memory limit enforced"
else
    echo "geo_bomb: UNEXPECTED: $out"
fi
