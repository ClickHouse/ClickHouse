#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Writing and reading the Parquet `FILE` logical type: a group annotated with `FILE` with the
# fields `uri`, `offset`, `size`, `content_type`, `checksum`, `inline` (any subset, all OPTIONAL).

FILE_DATA="${CLICKHOUSE_TMP}/file_logical_type.parquet"
PLAIN_DATA="${CLICKHOUSE_TMP}/file_logical_type_plain.parquet"

SOURCE="
    SELECT
        number AS id,
        CAST((
            if(number = 1, '', 's3://bucket/object_' || toString(number)),
            if(number = 0, 0, number * 100),
            number * 10,
            if(number = 2, 'image/png', 'application/octet-stream'),
            if(number = 3, NULL, 'sipHash128:' || lower(hex(sipHash128(toString(number))))),
            repeat('x', number)
        ) AS Tuple(uri String, offset UInt32, size UInt64, content_type LowCardinality(String), checksum Nullable(String), inline String)) AS file,
        CAST((toFixedString(char(number, 0, 255), 3), 'application/octet-stream') AS Tuple(inline FixedString(3), content_type String)) AS inline_file,
        arrayMap(i -> CAST(('file_' || toString(i), if(i = 1, NULL, i)) AS Tuple(uri String, size Nullable(Int64))), range(number)) AS files,
        if(number % 2 = 0, NULL, CAST(tuple('https://example.com/' || toString(number)) AS Tuple(uri String))) AS maybe_file,
        map('a', CAST(tuple(toInt8(-number)) AS Tuple(offset Int8))) AS file_map,
        CAST(('not a file', number) AS Tuple(uri String, other UInt64)) AS not_file
    FROM numbers(4)"

STRUCTURE="id UInt64,
    file Tuple(uri String, offset UInt32, size UInt64, content_type LowCardinality(String), checksum Nullable(String), inline String),
    inline_file Tuple(inline FixedString(3), content_type String),
    files Array(Tuple(uri String, size Nullable(Int64))),
    maybe_file Tuple(uri Nullable(String)),
    file_map Map(String, Tuple(offset Int8)),
    not_file Tuple(uri String, other UInt64)"

$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) ${SOURCE}"
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('${PLAIN_DATA}', Parquet) ${SOURCE}"

echo "--- schema inference with FILE groups"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE_DATA}')"

echo "--- schema inference without FILE groups"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${PLAIN_DATA}')"

echo "--- number of FILE annotations in the footer"
# In the Thrift compact protocol, `LogicalType` with its union field 19 (`FILE`) set to an empty
# `FileType` is encoded as 0x0C 0x26 (field header: struct, id 19) 0x00 (end of `FileType`)
# 0x00 (end of `LogicalType`). Expected for `file`, `inline_file`, `files`, `maybe_file`, `file_map`.
$CLICKHOUSE_LOCAL -q "SELECT countSubstrings(raw_blob, unhex('0C260000')) FROM file('${FILE_DATA}', RawBLOB)"
$CLICKHOUSE_LOCAL -q "SELECT countSubstrings(raw_blob, unhex('0C260000')) FROM file('${PLAIN_DATA}', RawBLOB)"

echo "--- data"
$CLICKHOUSE_LOCAL -q "SELECT * EXCEPT inline_file, hex(inline_file.inline), inline_file.content_type FROM file('${FILE_DATA}') ORDER BY id FORMAT Vertical"

echo "--- round trip with the original structure"
# A null `maybe_file` is a null FILE reference, which is read as a tuple of NULLs (the reader does not
# read an OPTIONAL group with OPTIONAL fields as `Nullable(Tuple)`).
$CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(cityHash64(id, file, inline_file, files, file_map, not_file, maybe_file.uri))
        = (SELECT sum(cityHash64(id, file, inline_file, files, file_map, not_file, maybe_file.uri)) FROM (${SOURCE}))
    FROM file('${FILE_DATA}', Parquet, '${STRUCTURE}')"

echo "--- subset of fields, reordered"
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM file('${FILE_DATA}', Parquet, 'id UInt64, file Tuple(size Nullable(Int64), uri Nullable(String))') ORDER BY id"

echo "--- subcolumns"
$CLICKHOUSE_LOCAL -q "SELECT id, file.uri, file.offset, files.size, maybe_file.uri FROM file('${FILE_DATA}') ORDER BY id"
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM file('${FILE_DATA}', Parquet, '\`file.content_type\` Nullable(String), \`files.uri\` Array(Nullable(String))') ORDER BY length(\`files.uri\`)"

echo "--- string fields are written as STRING and inline as plain BYTE_ARRAY regardless of output_format_parquet_string_as_string"
# The reader validates the annotations of the fields of a FILE group, so a successful DESCRIBE checks them.
for string_as_string in 0 1
do
    $CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 --output_format_parquet_string_as_string=$string_as_string -q "
        INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST(('a', 'b') AS Tuple(uri String, inline String)) AS f SETTINGS engine_file_truncate_on_insert = 1"
    $CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE_DATA}')"
done

echo "--- tuples that are not written as FILE"
# Element names must be FILE fields (case sensitively), so unnamed tuples are ordinary groups.
$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT tuple('a', 1) AS unnamed, CAST(tuple('a') AS Tuple(URI String)) AS upper_case
    SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL -q "SELECT countSubstrings(raw_blob, unhex('0C260000')) FROM file('${FILE_DATA}', RawBLOB)"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('${FILE_DATA}')"

echo "--- incompatible element types"
$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST(('a', 1.5) AS Tuple(uri String, size Float64)) AS f
    SETTINGS engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST((1, 1) AS Tuple(uri UInt64, size UInt64)) AS f
    SETTINGS engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST(tuple(true) AS Tuple(size Bool)) AS f
    SETTINGS engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

echo "--- a size that does not fit into INT64"
$CLICKHOUSE_LOCAL --output_format_parquet_write_file_logical_type=1 -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST(tuple(18446744073709551615) AS Tuple(size UInt64)) AS f
    SETTINGS engine_file_truncate_on_insert = 1" 2>&1 | grep -o -m1 'CANNOT_CONVERT_TYPE'

echo "--- the same tuples are written as ordinary groups without the setting"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('${FILE_DATA}', Parquet) SELECT CAST(('a', 1.5) AS Tuple(uri String, size Float64)) AS f
    SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('${FILE_DATA}')"

rm -f "${FILE_DATA}" "${PLAIN_DATA}"
