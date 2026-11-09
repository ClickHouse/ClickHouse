#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

file=${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet
CHL="$CLICKHOUSE_LOCAL --input_format_parquet_allow_missing_columns=1"

$CHL -q "insert into function file('$file', Parquet, 'x Array(Tuple(x Int64, y String))') select arrayMap(i -> (number * 10 + i, toString(number)||'-'||toString(i)), range(number)) from numbers(3)"

$CHL -q "select 'all:', * from file('$file')"
$CHL -q "select 'missing in Tuple:', * from file('$file', Parquet, 'x Array(Tuple(x Int64, z String))')"
$CHL -q "select 'missing with dot:', * from file('$file', Parquet, '\`x.x\` Array(Int64), \`x.y\` Array(String), \`x.z\` Array(String)')"
$CHL -q "select 'all elements missing:', * from file('$file', Parquet, 'x Array(Tuple(z String))')"

# The arrays of a missing element have the sizes of the arrays in the file.
$CHL -q "select 'missing with dot, alone:', * from file('$file', Parquet, '\`x.z\` Array(String)')"
$CHL -q "select 'missing with dot, beside one:', * from file('$file', Parquet, '\`x.y\` Array(String), \`x.z\` Array(String)')"
$CHL -q "select 'missing with dot, default:', * from file('$file', Parquet, '\`x.y\` Array(String), \`x.z\` Array(String) DEFAULT arrayMap(s -> ''d'', \`x.y\`)') settings input_format_defaults_for_omitted_fields = 1"
$CHL -q "select 'array join:', count() from file('$file', Parquet, '\`x.z\` Array(String)') array join \`x.z\`"
$CHL -q "select 'where:', count() from file('$file', Parquet, '\`x.z\` Array(String)') where length(\`x.z\`) > 0 settings optimize_move_to_prewhere = 0"
$CHL -q "select 'prewhere:', count() from file('$file', Parquet, '\`x.z\` Array(String)') prewhere length(\`x.z\`) > 0"
$CLICKHOUSE_LOCAL -q "select * from file('$file', Parquet, '\`x.z\` Array(String)') settings input_format_parquet_allow_missing_columns = 0" 2>&1 | grep -o -m1 'Code: [0-9]*'

nested_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_nested.parquet
$CHL -q "insert into function file('$nested_file', Parquet) select [[(1, 'a')], []]::Array(Array(Tuple(x Int64, y String))) as n"
$CHL -q "select 'nested arrays, all elements missing:', * from file('$nested_file', Parquet, 'n Array(Array(Tuple(z String)))')"
$CHL -q "select 'nested arrays, missing with dot:', * from file('$nested_file', Parquet, '\`n.z\` Array(Array(String))')"

struct_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_struct.parquet
$CHL -q "insert into function file('$struct_file', Parquet, 's Array(Tuple(t Tuple(a Int64)))') select arrayMap(i -> tuple(tuple(number * 10 + i)), range(number)) from numbers(3)"
$CHL -q "select 'tuple in element, all elements missing:', * from file('$struct_file', Parquet, 's Array(Tuple(t Tuple(b String)))')"
$CHL -q "select 'tuple in element, missing with dot:', * from file('$struct_file', Parquet, '\`s.t.b\` Array(String)')"
$CHL -q "select 'tuple in element with dot, all elements missing:', * from file('$struct_file', Parquet, '\`s.t\` Array(Tuple(b String))')"

# The leaf read for the array sizes holds a NULL of a type that cannot be Nullable (`Geometry`).
geo_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_geo.parquet
python3 - "$geo_file" <<'PY'
import sys

import pyarrow as pa
import pyarrow.parquet as pq


# Makes Arrow write the `GEOMETRY` logical type.
class WKB(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.binary(), "geoarrow.wkb")

    def __arrow_ext_serialize__(self):
        return b"{}"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return WKB()


geometry = pa.ExtensionArray.from_storage(WKB(), pa.array([None], pa.binary()))
element = pa.StructArray.from_arrays([geometry], fields=[pa.field("g", WKB())])
x = pa.ListArray.from_arrays(pa.array([0, 1], pa.int32()), element, type=pa.list_(pa.field("element", element.type, nullable=False)))
pq.write_table(pa.table({"x": x}), sys.argv[1])
PY
$CHL -q "select 'geometry leaf, all elements missing:', * from file('$geo_file', Parquet, 'x Array(Tuple(z Nullable(Int32)))') settings input_format_null_as_default = 0"
$CHL -q "select 'geometry leaf, missing with dot:', * from file('$geo_file', Parquet, '\`x.z\` Array(Nullable(Int32))') settings input_format_null_as_default = 0"

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

# With case-insensitive matching, `x.z` matches both arrays, like `x.a` does, even though `X` has more arrays.
case_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_case.parquet
$CHL -q "insert into function file('$case_file', Parquet) select [[(1)]]::Array(Array(Tuple(a Int32))) as X, [(1), (2)]::Array(Tuple(a Int32)) as x"
$CHL -q "select 'case-sensitive:', * from file('$case_file', Parquet, '\`x.z\` Array(Nullable(Int32))')"
$CHL -q "select * from file('$case_file', Parquet, '\`x.z\` Array(Nullable(Int32))') settings input_format_parquet_case_insensitive_column_matching = 1" 2>&1 | grep -o -m1 'Code: [0-9]*'

# The leaf read for the sizes is a fixed-width one, even if it is read for another column, rather than a long string.
cost_file=${CLICKHOUSE_TEST_UNIQUE_NAME}_cost.parquet
$CHL -q "insert into function file('$cost_file', Parquet) select [(number, repeat(toString(number), 100000))]::Array(Tuple(id UInt64, payload String)) as x from numbers(10) settings output_format_parquet_compression_method = 'zstd'"
$CHL --print-profile-events -q "select * from file('$cost_file', Parquet, '\`x.id\` Array(UInt64), \`x.z\` Array(String)') format Null" 2>&1 \
    | grep -o 'ParquetDecompressedBytes: [0-9]*' | awk '{ bytes += $2 } END { print "decompressed some but less than 100000 bytes:", (bytes > 0 && bytes < 100000) }'

rm -r $file $nested_file $struct_file $geo_file $iceberg_dir $case_file $cost_file
