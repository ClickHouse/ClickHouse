#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

FILE="${WORK_DIR}/schema_cache.parquet"

"${CLICKHOUSE_LOCAL}" --multiquery --query "
SET enable_json_type = 1;
SET output_format_parquet_use_custom_encoder = 1;
SET output_format_parquet_json_as_variant = 1;

SELECT CAST('{\"a\":1}' AS JSON(max_dynamic_paths=0)) AS j
INTO OUTFILE '${FILE}'
FORMAT Parquet;
"

python3 - "$FILE" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
old = b"ClickHouse.variant_type_hints"
new = b"ClickHouse.variant_type_hintx"

data = path.read_bytes()
if data.count(old) != 1:
    raise RuntimeError(f"expected one type-hint metadata key, got {data.count(old)}")
path.write_bytes(data.replace(old, new))
PY

"${CLICKHOUSE_LOCAL}" --multiquery --query "
SET enable_json_type = 1;
SET input_format_parquet_use_native_reader_v3 = 1;
SET input_format_parquet_enable_json_parsing = 1;
SET schema_inference_use_cache_for_file = 1;
SET schema_inference_make_columns_nullable = 1;
SET schema_inference_make_json_columns_nullable = 0;

DESC file('${FILE}', Parquet)
FORMAT TSVRaw;

SET schema_inference_make_json_columns_nullable = 1;

DESC file('${FILE}', Parquet)
FORMAT TSVRaw;
" | awk -F '\t' '{ print $1 "\t" $2 }'
