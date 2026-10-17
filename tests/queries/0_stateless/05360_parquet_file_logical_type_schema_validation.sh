#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Validation of the schema of groups annotated with the Parquet `FILE` logical type.
# The files are produced by writing an ordinary group with ClickHouse and adding the `FILE`
# annotation to its schema element in the footer, because writers that support `FILE` do not
# produce invalid schemas.

SRC="${CLICKHOUSE_TMP}/file_logical_type_validation_src.parquet"
DST="${CLICKHOUSE_TMP}/file_logical_type_validation.parquet"

# Adds `logicalType = FILE` to the schema element named by the first argument.
# In the Thrift compact protocol, ClickHouse writes a group element as: repetition_type (field 3),
# name (field 4, header 0x18), num_children (field 5, header 0x15), stop (0x00), and a primitive
# element without annotations as: type (field 1), repetition_type (field 3), name (field 4), stop.
# The annotation is field 10 (header 0x5C: delta 5 from num_children, or 0x6C: delta 6 from name,
# struct) holding a `LogicalType` union with field 19 (`FILE`) set to an empty struct: 0x0C 0x26 0x00 0x00.
annotate()
{
    python3 -I -c '
import re, struct, sys
src, name, dst = sys.argv[1], sys.argv[2].encode(), sys.argv[3]
data = open(src, "rb").read()
assert data[-4:] == b"PAR1"
footer_length = struct.unpack("<I", data[-8:-4])[0]
footer = data[-8 - footer_length:-8]
matches = list(re.finditer(re.escape(bytes([0x18, len(name)]) + name), footer))
# Skip the occurrences in `path_in_schema` of the column chunks: in a schema element the name
# directly follows repetition_type (header 0x25 after type, or 0x35 as the first field).
matches = [m for m in matches if m.start() >= 2 and footer[m.start() - 2] in (0x25, 0x35)]
assert len(matches) == 1
pos = matches[0].end()
header = 0x6C
if footer[pos] == 0x15:
    header = 0x5C
    pos += 1
    while footer[pos] & 0x80:
        pos += 1
    pos += 1
assert footer[pos] == 0
footer = footer[:pos] + bytes([header, 0x0C, 0x26, 0x00, 0x00]) + footer[pos:]
open(dst, "wb").write(data[:-8 - footer_length] + footer + struct.pack("<I", len(footer)) + b"PAR1")
' "$SRC" "$1" "$DST"
}

# Writes a file with columns `id` and `fileref` of the given type, and annotates `fileref` with `FILE`.
make_file()
{
    local type=$1
    shift
    $CLICKHOUSE_LOCAL "$@" -q "
        INSERT INTO FUNCTION file('${SRC}', Parquet) SELECT number AS id, CAST(defaultValueOfTypeName('${type}') AS ${type}) AS fileref FROM numbers(2)
        SETTINGS engine_file_truncate_on_insert = 1"
    annotate fileref
}

expect_error()
{
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('${DST}')" 2>&1 | grep -o -m1 "INCORRECT_DATA"
    # The schema is invalid as a whole, so reading only other columns fails as well.
    $CLICKHOUSE_LOCAL -q "SELECT id FROM file('${DST}')" 2>&1 | grep -o -m1 "$1"
}

echo "--- valid"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${SRC}', Parquet)
    SELECT number AS id, CAST(('s3://bucket/' || toString(number), if(number = 0, NULL, number)) AS Tuple(uri Nullable(String), size Nullable(Int64))) AS fileref
    FROM numbers(2) SETTINGS engine_file_truncate_on_insert = 1"
annotate fileref
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${DST}')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${DST}')"

echo "--- unrecognized field"
make_file 'Tuple(uri Nullable(String), url Nullable(String))'
expect_error 'contains unrecognized field `url`'

echo "--- field names are case sensitive"
make_file 'Tuple(URI Nullable(String))'
expect_error 'contains unrecognized field `URI`'

echo "--- REQUIRED field"
make_file 'Tuple(uri String)'
expect_error 'its field `uri` is not OPTIONAL'

echo "--- INT32 size"
make_file 'Tuple(size Nullable(Int32))'
expect_error 'its field `size` has unexpected type (expected INT64)'

echo "--- UINT_64 size"
make_file 'Tuple(size Nullable(UInt64))'
expect_error 'its field `size` has unexpected type (expected INT64)'

echo "--- uri without STRING annotation"
make_file 'Tuple(uri Nullable(String))' --output_format_parquet_string_as_string=0
expect_error 'its field `uri` has unexpected type (expected BYTE_ARRAY annotated with STRING)'

echo "--- inline with STRING annotation"
make_file 'Tuple(inline Nullable(String))' --output_format_parquet_string_as_string=1
expect_error 'its field `inline` has unexpected type (expected BYTE_ARRAY without annotation)'

echo "--- nested group"
make_file 'Tuple(uri Nullable(String), checksum Tuple(a Nullable(String)))'
expect_error 'its field `checksum` is a group'

echo "--- primitive column"
make_file 'Int64'
expect_error 'is a primitive column rather than a group'

rm -f "${SRC}" "${DST}"
