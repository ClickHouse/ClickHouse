#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression: Parquet/Arrow read with hint Nullable(Array(T)) must use the nullable
# decode path so list-level nulls are preserved in the outer null map.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

PARQUET_FILE="$TMP_DIR/nullable_list.parquet"

python3 - "$PARQUET_FILE" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
arr = pa.array([[1, 2], None, [], [3, 4]], type=pa.list_(pa.int32()))
pq.write_table(pa.table({"arr": arr}), path)
PYEOF

$CLICKHOUSE_LOCAL -q "
SET allow_experimental_nullable_array_type = 1;
SELECT throwIf(count() != 4 OR sum(isNull(arr)) != 1 OR countIf(length(arr) IS NULL) != 1 OR sum(length(arr)) != 4)
FROM file('$PARQUET_FILE', Parquet, 'arr Nullable(Array(Int32))')
FORMAT Null
"

result=$($CLICKHOUSE_LOCAL -q "
SET allow_experimental_nullable_array_type = 1;
DESC format(JSONEachRow, '{\"arr\": null}
{\"arr\": [1,2]}') SETTINGS schema_inference_make_columns_nullable = 1
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in schema inference, got: $result"
    exit 1
}
