#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS arrow_views_test"

$CLICKHOUSE_CLIENT -q "CREATE TABLE arrow_views_test (id UInt32, text Nullable(String), bin_data Nullable(String)) ENGINE = MergeTree() ORDER BY id"

# Use literal python3, pipe the binary output straight into the INSERT
OPENSSL_CONF=/dev/null python3 - <<'EOF' | $CLICKHOUSE_CLIENT -q "INSERT INTO arrow_views_test FORMAT Arrow"
import sys
import pyarrow as pa

data = [
    pa.array([1, 2, 3, 4], type=pa.uint32()),
    
    # Column 1: STRING_VIEW (Standard Python strings)
    pa.array([
        "Short", 
        "A significantly longer string that forces out-of-line storage", 
        None,
        "Another significantly longer string that forces out-of-line storage after null"
    ], type=pa.string_view()),
    
    # Column 2: BINARY_VIEW (Python bytes objects with b"prefix")
    pa.array([
        b"Bin", 
        b"A significantly longer binary sequence for out-of-line", 
        None,
        b"Another significantly longer binary sequence after null"
    ], type=pa.binary_view())
]

batch = pa.RecordBatch.from_arrays(data, names=['id', 'text', 'bin_data'])

with pa.RecordBatchFileWriter(sys.stdout.buffer, batch.schema) as writer:
    writer.write_batch(batch)
EOF

$CLICKHOUSE_CLIENT -q "SELECT * FROM arrow_views_test ORDER BY id"
$CLICKHOUSE_CLIENT -q "DROP TABLE arrow_views_test"