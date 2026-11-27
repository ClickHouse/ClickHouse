#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs pyarrow and the IcebergLocal engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CHL="$CLICKHOUSE_LOCAL --input_format_parquet_allow_missing_columns=1"

# The leaf read for the array sizes is not decoded with its logical type: an `INT96` timestamp and a
# `DATE` out of the range of `DateTime64(9)` and `Date32`.
range_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_range.parquet
python3 - "$range_file" <<'PY'
import datetime
import sys

import pyarrow as pa
import pyarrow.parquet as pq

x = pa.array([[{"old": datetime.datetime(3000, 1, 1)}]], pa.list_(pa.struct([("old", pa.timestamp("us"))])))
y = pa.array([[{"old": 1 << 30}]], pa.list_(pa.struct([("old", pa.date32())])))
pq.write_table(pa.table({"x": x, "y": y}), sys.argv[1], use_deprecated_int96_timestamps=True)
PY
$CHL -q "select 'values out of range, all elements missing:', * from file('$range_file', Parquet, 'x Array(Tuple(q Int32)), y Array(Tuple(q Int32))')"
$CHL -q "select 'values out of range, missing with dot:', * from file('$range_file', Parquet, '\`x.q\` Array(Int32), \`y.q\` Array(Int32)')"

# An Iceberg list that is dropped and added again keeps its old field ids in the old data files. An
# element of the new list is not in such a file, and does not get the sizes of the old list.
iceberg_dir=${CLICKHOUSE_TEST_UNIQUE_NAME}_iceberg
$CHL --allow_insert_into_iceberg=1 -q "
    create table t (id Int32, x Array(Tuple(old Nullable(Int32)))) engine = IcebergLocal('$iceberg_dir/');
    insert into t values (1, [(11)]), (2, [(21), (22)])"
# Replace `x` with a new `x` and bind the snapshot to the new schema, so that the file is read under it.
python3 - "$iceberg_dir/metadata" <<'PY'
import json
import os
import re
import sys

metadata_dir = sys.argv[1]
version = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    metadata = json.load(f)
schema = json.loads(json.dumps(metadata["schemas"][-1]))
last_id = metadata["last-column-id"]
new_element = {"type": "struct", "fields": [{"id": last_id + 3, "name": "new", "required": False, "type": "int"}]}
new_x = {"type": "list", "element-id": last_id + 2, "element-required": True, "element": new_element}
schema["fields"] = [field for field in schema["fields"] if field["name"] != "x"]
schema["fields"].append({"id": last_id + 1, "name": "x", "required": False, "type": new_x})
schema["schema-id"] = 1
metadata["schemas"].append(schema)
metadata["current-schema-id"] = 1
metadata["last-column-id"] = last_id + 3
for snapshot in metadata["snapshots"]:
    snapshot["schema-id"] = 1
with open(os.path.join(metadata_dir, f"v{version + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f)
PY
$CHL -q "select 'list added again, whole:', id, x from icebergLocal('$iceberg_dir/') order by id"
$CHL -q "select 'list added again, element:', id, x.new from icebergLocal('$iceberg_dir/') order by id"
$CHL -q "select 'list added again, array join:', count() from icebergLocal('$iceberg_dir/') array join x.new"

# The same file under two schemas of the original `x`: in v101 its leaf is the element `new`, in v102
# `new` is another column, so the leaf gives only the sizes. The columns cache keeps them apart.
python3 - "$iceberg_dir/metadata" <<'PY'
import copy
import json
import os
import re
import sys

metadata_dir = sys.argv[1]
version = max(int(m.group(1)) for f in os.listdir(metadata_dir) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
with open(os.path.join(metadata_dir, f"v{version}.metadata.json")) as f:
    metadata = json.load(f)
last_id = metadata["last-column-id"]
original_x = next(field for field in metadata["schemas"][0]["fields"] if field["name"] == "x")
for schema_id, field_id in [(101, original_x["type"]["element"]["fields"][0]["id"]), (102, last_id + 1)]:
    schema = copy.deepcopy(metadata["schemas"][0])
    x = next(field for field in schema["fields"] if field["name"] == "x")
    x["type"]["element"]["fields"] = [{"id": field_id, "name": "new", "required": False, "type": "int"}]
    schema["schema-id"] = schema_id
    result = copy.deepcopy(metadata)
    result["schemas"].append(schema)
    result["current-schema-id"] = schema_id
    result["last-column-id"] = last_id + 1
    for snapshot in result["snapshots"]:
        snapshot["schema-id"] = schema_id
    with open(os.path.join(metadata_dir, f"v{schema_id}.metadata.json"), "w") as f:
        json.dump(result, f)
PY
cache="use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1"
$CHL -q "
    select 'element renamed:', id, x from icebergLocal('$iceberg_dir/', settings iceberg_metadata_file_path = 'metadata/v101.metadata.json') order by id settings $cache;
    select 'element added again:', id, x from icebergLocal('$iceberg_dir/', settings iceberg_metadata_file_path = 'metadata/v102.metadata.json') order by id settings $cache;
    select 'cache entries of x:', count() from system.columns_cache where database = '' and column = 'x'"

rm -r $range_file $iceberg_dir
