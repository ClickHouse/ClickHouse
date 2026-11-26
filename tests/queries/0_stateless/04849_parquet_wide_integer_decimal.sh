#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LEGACY_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_legacy.parquet"
DECIMAL_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_decimal.parquet"
INFER_39_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_infer_39.parquet"
SCALE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_scale.parquet"
CORRUPT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_corrupt.parquet"
BAD_STATS_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_bad_stats.parquet"
EXTERNAL_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_external.parquet"
INVALID_WIDTH_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_invalid_width.parquet"
INT256_OVERFLOW_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_int256_overflow.parquet"
ROW_GROUP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_row_groups.parquet"
PAGE_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pages.parquet"
trap 'rm -f "${LEGACY_FILE}" "${DECIMAL_FILE}" "${INFER_39_FILE}" "${SCALE_FILE}" "${CORRUPT_FILE}" "${BAD_STATS_FILE}" "${EXTERNAL_FILE}" "${INVALID_WIDTH_FILE}" "${INT256_OVERFLOW_FILE}" "${ROW_GROUP_FILE}" "${PAGE_FILE}"' EXIT

write_boundaries()
{
    local output_file="$1"
    local use_decimal="$2"
    ${CLICKHOUSE_LOCAL} --query="
        WITH
            multiIf(
                number = 0, toUInt128(0),
                number = 1, toUInt128(1),
                number = 2, bitShiftLeft(toUInt128(1), 127),
                number = 3, toUInt128('340282366920938463463374607431768211454'),
                number = 4, toUInt128('340282366920938463463374607431768211455'),
                toUInt128('1512366075204170947332355369683137040')) AS u128,
            multiIf(
                number = 0, toUInt256(0),
                number = 1, toUInt256(1),
                number = 2, bitShiftLeft(toUInt256(1), 255),
                number = 3, toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639934'),
                number = 4, toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'),
                toUInt256('514631507721405312519378913364952599437893773118507240507895286931579072255')) AS u256,
            multiIf(
                number = 0, toInt128('-170141183460469231731687303715884105728'),
                number = 1, toInt128(-1),
                number = 2, toInt128(0),
                number = 3, toInt128(1),
                number = 4, toInt128('170141183460469231731687303715884105727'),
                toInt128('-24197857203266734864629346612071973666')) AS i128,
            multiIf(
                number = 0, toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'),
                number = 1, toInt256(-1),
                number = 2, toInt256(0),
                number = 3, toInt256(1),
                number = 4, toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'),
                toInt256('-514631507721405312519378913364952599457899916736173488040770672359315992320')) AS i256
        SELECT
            number AS n,
            u128,
            u256,
            i128,
            i256,
            if(number = 2, NULL, u128)::Nullable(UInt128) AS u128_nullable,
            [i128] AS i128_array,
            tuple(u256, i256) AS wide_tuple
        FROM numbers(6)
        SETTINGS
            output_format_parquet_wide_integer_as_decimal = ${use_decimal},
            output_format_parquet_compression_method = 'none',
            output_format_parquet_write_checksums = 0,
            output_format_parquet_write_bloom_filter = 0,
            max_block_size = 1000000
        FORMAT Parquet
    " > "${output_file}"
}

write_boundaries "${LEGACY_FILE}" 0
write_boundaries "${DECIMAL_FILE}" 1

# Inspect the compact-Thrift footer and uncompressed page bytes without using either ClickHouse
# Parquet reader. This independently checks both decimal annotations, exact widths, standard
# column-chunk statistics, and representative payloads. It also creates a file whose standard
# minimum statistic has the wrong width while leaving the data and schema intact.
python3 - "${LEGACY_FILE}" "${DECIMAL_FILE}" "${BAD_STATS_FILE}" <<'PY'
import pathlib
import struct
import sys

legacy_path, decimal_path, bad_stats_path = map(pathlib.Path, sys.argv[1:])

def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7f
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)

def zigzag_decode(value):
    return (value >> 1) ^ -(value & 1)

class Walker:
    def __init__(self, data, position):
        self.data = data
        self.position = position
        self.records = {}

    def byte(self):
        value = self.data[self.position]
        self.position += 1
        return value

    def varint(self):
        result = 0
        shift = 0
        while True:
            byte = self.byte()
            result |= (byte & 0x7f) << shift
            if not byte & 0x80:
                return result
            shift += 7

    def struct(self, path):
        last_field = 0
        while True:
            header = self.byte()
            if header == 0:
                return
            delta = header >> 4
            compact_type = header & 0x0f
            field = zigzag_decode(self.varint()) if delta == 0 else last_field + delta
            last_field = field
            self.field(compact_type, path + (field,))

    def field(self, compact_type, path):
        value_start = self.position
        value = None
        if compact_type == 0x01:
            value = True
        elif compact_type == 0x02:
            value = False
        elif compact_type == 0x03:
            value = self.byte()
        elif compact_type in (0x04, 0x05, 0x06):
            value = zigzag_decode(self.varint())
        elif compact_type == 0x07:
            self.position += 8
        elif compact_type == 0x08:
            length = self.varint()
            value = bytes(self.data[self.position:self.position + length])
            self.position += length
        elif compact_type in (0x09, 0x0a):
            self.list(path)
        elif compact_type == 0x0c:
            self.struct(path)
        else:
            raise ValueError(f"unsupported compact type {compact_type}")
        if compact_type <= 0x08:
            self.records[path] = (compact_type, value, value_start, self.position)

    def list(self, path):
        header = self.byte()
        size = header >> 4
        element_type = header & 0x0f
        if size == 0x0f:
            size = self.varint()
        for index in range(size):
            self.field(element_type, path + (index,))

def footer_records(data):
    assert data[:4] == b"PAR1" and data[-4:] == b"PAR1"
    footer_length = struct.unpack("<I", data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_length
    walker = Walker(data, footer_start)
    walker.struct(())
    assert walker.position == len(data) - 8
    return footer_start, walker.records

def record_value(records, path):
    return records[path][1]

def schema_elements_by_name(records):
    result = {}
    for path, (_, value, _, _) in records.items():
        if len(path) == 3 and path[0] == 2 and path[2] == 4:
            result[value.decode()] = path[1]
    return result

legacy = legacy_path.read_bytes()
decimal = decimal_path.read_bytes()
legacy_footer, legacy_records = footer_records(legacy)
decimal_footer, decimal_records = footer_records(decimal)
legacy_schema = schema_elements_by_name(legacy_records)
decimal_schema = schema_elements_by_name(decimal_records)

expected = {
    "u128": (16, 17, 39),
    "u256": (32, 33, 78),
    "i128": (16, 17, 39),
    "i256": (32, 33, 77),
}
for name, (legacy_width, decimal_width, precision) in expected.items():
    legacy_index = legacy_schema[name]
    assert record_value(legacy_records, (2, legacy_index, 1)) == 7  # `FIXED_LEN_BYTE_ARRAY`
    assert record_value(legacy_records, (2, legacy_index, 2)) == legacy_width
    assert (2, legacy_index, 6) not in legacy_records
    assert (2, legacy_index, 10, 5, 1) not in legacy_records

    decimal_index = decimal_schema[name]
    base = (2, decimal_index)
    assert record_value(decimal_records, base + (1,)) == 7  # `FIXED_LEN_BYTE_ARRAY`
    assert record_value(decimal_records, base + (2,)) == decimal_width
    assert record_value(decimal_records, base + (6,)) == 5  # `ConvertedType.DECIMAL`
    assert record_value(decimal_records, base + (7,)) == 0
    assert record_value(decimal_records, base + (8,)) == precision
    assert record_value(decimal_records, base + (10, 5, 1)) == 0
    assert record_value(decimal_records, base + (10, 5, 2)) == precision

# The file is uncompressed and dictionary entries use `PLAIN` encoding, so these sequences are actual
# physical values independently of page decoding.
body = decimal[4:decimal_footer]
for payload in (
    bytes.fromhex("00" + "ff" * 16),
    bytes.fromhex("ff80" + "00" * 15),
    bytes.fromhex("00" + "ff" * 32),
    bytes.fromhex("ff80" + "00" * 31),
    bytes.fromhex("000123456789abcdeffedcba9876543210"),
    bytes.fromhex("000123456789abcdeffedcba987654321000112233445566778899aabbccddeeff"),
    bytes.fromhex("ffedcba9876543210ff0123456789abcde"),
    bytes.fromhex("fffedcba98765432100123456789abcdeff0e1d2c3b4a596877766554433221100"),
):
    assert payload in body

# Check standard row-group statistics directly in `ColumnMetaData.statistics`.
columns = {}
for path, (_, value, _, _) in decimal_records.items():
    if len(path) == 7 and path[:3] == (4, 0, 1) and path[4:] == (3, 3, 0):
        columns[value.decode()] = path[3]
statistics = {
    "u128": (bytes(17), bytes.fromhex("00" + "ff" * 16)),
    "u256": (bytes(33), bytes.fromhex("00" + "ff" * 32)),
    "i128": (bytes.fromhex("ff80" + "00" * 15), bytes.fromhex("007f" + "ff" * 15)),
    "i256": (bytes.fromhex("ff80" + "00" * 31), bytes.fromhex("007f" + "ff" * 31)),
}
for name, (minimum, maximum) in statistics.items():
    column_index = columns[name]
    stats_path = (4, 0, 1, column_index, 3, 12)
    assert record_value(decimal_records, stats_path + (6,)) == minimum
    assert record_value(decimal_records, stats_path + (5,)) == maximum

# Shorten only the `u128` minimum statistic from 17 bytes to 16 bytes and update the footer length.
bad_path = (4, 0, 1, columns["u128"], 3, 12, 6)
_, value, start, end = decimal_records[bad_path]
replacement = encode_varint(16) + value[:16]
footer = decimal[decimal_footer:-8]
relative_start = start - decimal_footer
relative_end = end - decimal_footer
bad_footer = footer[:relative_start] + replacement + footer[relative_end:]
bad_stats_path.write_bytes(decimal[:decimal_footer] + bad_footer + struct.pack("<I", len(bad_footer)) + b"PAR1")

print("raw footer schema, payload, and statistics: OK")
PY

# Build a Parquet file from the compact-Thrift structures and `PLAIN` page payloads directly. This
# fixture is independent of both ClickHouse's writer and reader. In addition to the writer's
# 17/33-byte physical widths, it emits standard fixed and variable
# decimal encodings, valid one-byte decimals with each annotation form, an invalid width/precision
# schema, and a genuine runtime `Int256` overflow.
python3 - "${EXTERNAL_FILE}" "${INVALID_WIDTH_FILE}" "${INT256_OVERFLOW_FILE}" <<'PY'
import pathlib
import struct
import sys

output_path, invalid_width_path, int256_overflow_path = map(pathlib.Path, sys.argv[1:])

CT_TRUE = 1
CT_I32 = 5
CT_I64 = 6
CT_BINARY = 8
CT_LIST = 9
CT_STRUCT = 12

def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7f
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)

def integer(value):
    return varint((value << 1) ^ (value >> 63))

def binary(value):
    return varint(len(value)) + value

def thrift_struct(fields):
    out = bytearray()
    previous_id = 0
    for field_id, compact_type, value in fields:
        delta = field_id - previous_id
        assert 1 <= delta <= 15
        out.append((delta << 4) | compact_type)
        out += value
        previous_id = field_id
    out.append(0)
    return bytes(out)

def thrift_list(compact_type, values):
    out = bytearray()
    if len(values) < 15:
        out.append((len(values) << 4) | compact_type)
    else:
        out.append(0xf0 | compact_type)
        out += varint(len(values))
    for value in values:
        out += value
    return bytes(out)

def decimal_logical_type(precision):
    decimal_type = thrift_struct([
        (1, CT_I32, integer(0)),
        (2, CT_I32, integer(precision)),
    ])
    return thrift_struct([(5, CT_STRUCT, decimal_type)])

def schema_element(name, width, precision, annotation):
    fields = [
        (1, CT_I32, integer(6 if width is None else 7)),  # `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`
    ]
    if width is not None:
        fields.append((2, CT_I32, integer(width)))
    fields += [
        (3, CT_I32, integer(0)),  # `REQUIRED`
        (4, CT_BINARY, binary(name.encode())),
    ]
    if annotation in ("converted", "both"):
        fields += [
            (6, CT_I32, integer(5)),  # `ConvertedType.DECIMAL`
            (7, CT_I32, integer(0)),
            (8, CT_I32, integer(precision)),
        ]
    if annotation in ("logical", "both"):
        fields.append((10, CT_STRUCT, decimal_logical_type(precision)))
    return thrift_struct(fields)

def statistics(minimum, maximum):
    return thrift_struct([
        (5, CT_BINARY, binary(maximum)),
        (6, CT_BINARY, binary(minimum)),
        (7, CT_TRUE, b""),
        (8, CT_TRUE, b""),
    ])

columns = [
    (
        "u128",
        17,
        39,
        [
            bytes(17),
            bytes.fromhex("00" + "80" + "00" * 15),
            bytes.fromhex("00" + "ff" * 16),
            bytes.fromhex("000123456789abcdeffedcba9876543210"),
        ],
        "both",
    ),
    (
        "u256",
        33,
        78,
        [
            bytes(33),
            bytes.fromhex("00" + "80" + "00" * 31),
            bytes.fromhex("00" + "ff" * 32),
            bytes.fromhex("000123456789abcdeffedcba987654321000112233445566778899aabbccddeeff"),
        ],
        "both",
    ),
    (
        "i128",
        17,
        39,
        [
            bytes.fromhex("ff80" + "00" * 15),
            bytes(17),
            bytes.fromhex("007f" + "ff" * 15),
            bytes.fromhex("ffedcba9876543210ff0123456789abcde"),
        ],
        "both",
    ),
    (
        "i256",
        33,
        77,
        [
            bytes.fromhex("ff80" + "00" * 31),
            bytes(33),
            bytes.fromhex("007f" + "ff" * 31),
            bytes.fromhex("fffedcba98765432100123456789abcdeff0e1d2c3b4a596877766554433221100"),
        ],
        "both",
    ),
    (
        "fixed_18_u128",
        18,
        39,
        [
            bytes(18),
            bytes.fromhex("0000" + "80" + "00" * 15),
            bytes.fromhex("0000" + "ff" * 16),
            bytes.fromhex("00000123456789abcdeffedcba9876543210"),
        ],
        "both",
    ),
    (
        "byte_array_u256",
        None,
        78,
        [
            b"\x00",
            bytes.fromhex("00" + "80" + "00" * 31),
            bytes.fromhex("00" + "ff" * 32),
            bytes.fromhex("000000000000000123456789abcdeffedcba987654321000112233445566778899aabbccddeeff"),
        ],
        "both",
    ),
    ("logical_only", 1, 2, [b"\x9d", b"\x00", b"\x63", b"\xd6"], "logical"),
    ("converted_only", 1, 2, [b"\x9d", b"\x00", b"\x63", b"\xd6"], "converted"),
    ("short_unsigned", 1, 2, [b"\x00", b"\x01", b"\x63", b"\x2a"], "both"),
]

def write_file(path, file_columns):
    row_count = len(file_columns[0][3])
    assert all(len(column[3]) == row_count for column in file_columns)
    file_bytes = bytearray(b"PAR1")
    column_chunks = []
    total_byte_size = 0
    for name, width, _precision, values, _annotation in file_columns:
        if not values:
            continue
        physical_type = 6 if width is None else 7
        if width is None:
            payload = b"".join(struct.pack("<I", len(value)) + value for value in values)
        else:
            assert all(len(value) == width for value in values)
            payload = b"".join(values)
        page_statistics = statistics(min(values, key=lambda value: int.from_bytes(value, "big", signed=True)),
                                     max(values, key=lambda value: int.from_bytes(value, "big", signed=True)))
        data_page_header = thrift_struct([
            (1, CT_I32, integer(len(values))),
            (2, CT_I32, integer(0)),  # `PLAIN`
            (3, CT_I32, integer(3)),  # `RLE`
            (4, CT_I32, integer(3)),  # `RLE`
            (5, CT_STRUCT, page_statistics),
        ])
        page_header = thrift_struct([
            (1, CT_I32, integer(0)),  # `DATA_PAGE`
            (2, CT_I32, integer(len(payload))),
            (3, CT_I32, integer(len(payload))),
            (5, CT_STRUCT, data_page_header),
        ])
        page_offset = len(file_bytes)
        file_bytes += page_header + payload
        chunk_size = len(page_header) + len(payload)
        total_byte_size += chunk_size

        column_metadata = thrift_struct([
            (1, CT_I32, integer(physical_type)),
            (2, CT_LIST, thrift_list(CT_I32, [integer(0), integer(3)])),  # `PLAIN`, `RLE`
            (3, CT_LIST, thrift_list(CT_BINARY, [binary(name.encode())])),
            (4, CT_I32, integer(0)),  # `UNCOMPRESSED`
            (5, CT_I64, integer(len(values))),
            (6, CT_I64, integer(chunk_size)),
            (7, CT_I64, integer(chunk_size)),
            (9, CT_I64, integer(page_offset)),
            (12, CT_STRUCT, page_statistics),
        ])
        column_chunks.append(thrift_struct([
            (2, CT_I64, integer(page_offset)),
            (3, CT_STRUCT, column_metadata),
        ]))

    root_schema = thrift_struct([
        (4, CT_BINARY, binary(b"schema")),
        (5, CT_I32, integer(len(file_columns))),
    ])
    schema = [root_schema] + [
        schema_element(name, width, precision, annotation)
        for name, width, precision, _values, annotation in file_columns
    ]
    row_groups = []
    if row_count:
        row_groups.append(thrift_struct([
            (1, CT_LIST, thrift_list(CT_STRUCT, column_chunks)),
            (2, CT_I64, integer(total_byte_size)),
            (3, CT_I64, integer(row_count)),
            (6, CT_I64, integer(total_byte_size)),
        ]))
    file_metadata = thrift_struct([
        (1, CT_I32, integer(2)),
        (2, CT_LIST, thrift_list(CT_STRUCT, schema)),
        (3, CT_I64, integer(row_count)),
        (4, CT_LIST, thrift_list(CT_STRUCT, row_groups)),
        (6, CT_BINARY, binary(b"independent compact-Thrift fixture")),
    ])
    file_bytes += file_metadata + struct.pack("<I", len(file_metadata)) + b"PAR1"
    path.write_bytes(file_bytes)

write_file(output_path, columns)
write_file(invalid_width_path, [("invalid", 1, 3, [], "both")])
write_file(
    int256_overflow_path,
    [("overflow", 33, 77, [bytes.fromhex("0080" + "00" * 31)], "both")],
)
PY

STRUCTURE="n UInt64, u128 UInt128, u256 UInt256, i128 Int128, i256 Int256, u128_nullable Nullable(UInt128), i128_array Array(Int128), wide_tuple Tuple(UInt256, Int256)"
RAW_LEGACY_STRUCTURE="n UInt64, u128 FixedString(16), u256 FixedString(32), i128 FixedString(16), i256 FixedString(32), u128_nullable Nullable(UInt128), i128_array Array(Int128), wide_tuple Tuple(UInt256, Int256)"
RAW_DECIMAL_STRUCTURE="n UInt64, u128 FixedString(17), u256 FixedString(33), i128 FixedString(17), i256 FixedString(33), u128_nullable Nullable(UInt128), i128_array Array(Int128), wide_tuple Tuple(UInt256, Int256)"

echo "default legacy schema and payload"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        col.name,
        col.physical_type = 'FIXED_LEN_BYTE_ARRAY',
        positionCaseInsensitive(col.logical_type, 'decimal') = 0
    FROM (SELECT arrayJoin(columns) AS col FROM file('${LEGACY_FILE}', ParquetMetadata))
    WHERE col.name IN ('u128', 'u256', 'i128', 'i256')
    ORDER BY col.name
"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        countIf(length(u128) = 16 AND length(u256) = 32 AND length(i128) = 16 AND length(i256) = 32) = 6,
        countIf(n = 1 AND hex(u128) = concat('01', repeat('00', 15))) = 1,
        countIf(n = 1 AND hex(u256) = concat('01', repeat('00', 31))) = 1,
        countIf(n = 1 AND hex(i128) = repeat('FF', 16)) = 1,
        countIf(n = 1 AND hex(i256) = repeat('FF', 32)) = 1
    FROM file('${LEGACY_FILE}', Parquet, '${RAW_LEGACY_STRUCTURE}')
"
echo "legacy explicit wide-integer round trip"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        count(),
        min(u128), max(u128),
        min(u256), max(u256),
        min(i128), max(i128),
        min(i256), max(i256)
    FROM file('${LEGACY_FILE}', Parquet, '${STRUCTURE}')
"

echo "independently generated decimal fixture"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        min(u128), max(u128),
        min(u256), max(u256),
        min(i128), max(i128),
        min(i256), max(i256)
    FROM file('${EXTERNAL_FILE}', Parquet, 'u128 UInt128, u256 UInt256, i128 Int128, i256 Int256')
"
${CLICKHOUSE_LOCAL} --query="
    SELECT countIf(
        u128 = toUInt128('1512366075204170947332355369683137040')
        AND u256 = toUInt256('514631507721405312519378913364952599437893773118507240507895286931579072255')
        AND i128 = toInt128('-24197857203266734864629346612071973666')
        AND i256 = toInt256('-514631507721405312519378913364952599457899916736173488040770672359315992320')) = 1
    FROM file('${EXTERNAL_FILE}', Parquet, 'u128 UInt128, u256 UInt256, i128 Int128, i256 Int256')
"

echo "standard external fixed and variable decimal widths"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        min(fixed_18_u128), max(fixed_18_u128),
        min(byte_array_u256), max(byte_array_u256)
    FROM file('${EXTERNAL_FILE}', Parquet, 'fixed_18_u128 UInt128, byte_array_u256 UInt256')
"
${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${EXTERNAL_FILE}', Parquet, 'fixed_18_u128 UInt128, byte_array_u256 UInt256')
    WHERE fixed_18_u128 = toUInt128('1512366075204170947332355369683137040')
        AND byte_array_u256 = toUInt256('514631507721405312519378913364952599437893773118507240507895286931579072255')
    SETTINGS input_format_parquet_filter_push_down = 1
"

echo "modern logical decimal annotation and short signed width"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(logical_only), max(logical_only),
        countIf(logical_only = -99), countIf(logical_only = -42),
        countIf(logical_only = 0), countIf(logical_only = 99)
    FROM file('${EXTERNAL_FILE}', Parquet, 'logical_only Int128')
"
echo "deprecated converted decimal annotation and short signed width"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(converted_only), max(converted_only),
        countIf(converted_only = -99), countIf(converted_only = -42),
        countIf(converted_only = 0), countIf(converted_only = 99)
    FROM file('${EXTERNAL_FILE}', Parquet, 'converted_only Int256')
"
echo "short unsigned width extends to every target limb"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(short_unsigned), max(short_unsigned),
        countIf(short_unsigned = 0), countIf(short_unsigned = 1),
        countIf(short_unsigned = 42), countIf(short_unsigned = 99)
    FROM file('${EXTERNAL_FILE}', Parquet, 'short_unsigned UInt128')
"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(short_unsigned), max(short_unsigned),
        countIf(short_unsigned = 0), countIf(short_unsigned = 1),
        countIf(short_unsigned = 42), countIf(short_unsigned = 99)
    FROM file('${EXTERNAL_FILE}', Parquet, 'short_unsigned UInt256')
"

echo "decimal schema, payload, and standard statistics"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        col.name,
        col.physical_type = 'FIXED_LEN_BYTE_ARRAY',
        positionCaseInsensitive(col.logical_type, 'decimal') > 0,
        position(col.logical_type, multiIf(col.name IN ('u128', 'i128'), '39', col.name = 'i256', '77', '78')) > 0
    FROM (SELECT arrayJoin(columns) AS col FROM file('${DECIMAL_FILE}', ParquetMetadata))
    WHERE col.name IN ('u128', 'u256', 'i128', 'i256')
    ORDER BY col.name
"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        countIf(length(u128) = 17 AND length(u256) = 33 AND length(i128) = 17 AND length(i256) = 33) = 6,
        countIf(n = 1 AND hex(u128) = concat(repeat('00', 16), '01')) = 1,
        countIf(n = 1 AND hex(u256) = concat(repeat('00', 32), '01')) = 1,
        countIf(n = 2 AND hex(u128) = concat('0080', repeat('00', 15))) = 1,
        countIf(n = 2 AND hex(u256) = concat('0080', repeat('00', 31))) = 1,
        countIf(n = 0 AND hex(i128) = concat('FF80', repeat('00', 15))) = 1,
        countIf(n = 0 AND hex(i256) = concat('FF80', repeat('00', 31))) = 1,
        countIf(n = 4 AND hex(u128) = concat('00', repeat('FF', 16))) = 1,
        countIf(n = 4 AND hex(u256) = concat('00', repeat('FF', 32))) = 1,
        countIf(n = 4 AND hex(i128) = concat('007F', repeat('FF', 15))) = 1,
        countIf(n = 4 AND hex(i256) = concat('007F', repeat('FF', 31))) = 1,
        countIf(
            n = 5
            AND hex(u128) = '000123456789ABCDEFFEDCBA9876543210'
            AND hex(u256) = '000123456789ABCDEFFEDCBA987654321000112233445566778899AABBCCDDEEFF'
            AND hex(i128) = 'FFEDCBA9876543210FF0123456789ABCDE'
            AND hex(i256) = 'FFFEDCBA98765432100123456789ABCDEFF0E1D2C3B4A596877766554433221100') = 1
    FROM file('${DECIMAL_FILE}', Parquet, '${RAW_DECIMAL_STRUCTURE}')
"
${CLICKHOUSE_LOCAL} --query="
    WITH
        arrayStringConcat(arrayMap(x -> hex(toUInt8(x)), splitByChar(' ', trimRight(assumeNotNull(col.statistics.min))))) AS min_hex,
        arrayStringConcat(arrayMap(x -> hex(toUInt8(x)), splitByChar(' ', trimRight(assumeNotNull(col.statistics.max))))) AS max_hex
    SELECT
        col.name,
        min_hex = multiIf(
            col.name = 'u128', repeat('00', 17),
            col.name = 'u256', repeat('00', 33),
            col.name = 'i128', concat('FF80', repeat('00', 15)),
            concat('FF80', repeat('00', 31))),
        max_hex = multiIf(
            col.name = 'u128', concat('00', repeat('FF', 16)),
            col.name = 'u256', concat('00', repeat('FF', 32)),
            col.name = 'i128', concat('007F', repeat('FF', 15)),
            concat('007F', repeat('FF', 31)))
    FROM
    (
        SELECT arrayJoin(tupleElement(row_group, 'columns')) AS col
        FROM
        (
            SELECT arrayJoin(row_groups) AS row_group
            FROM file('${DECIMAL_FILE}', ParquetMetadata)
        )
    )
    WHERE col.name IN ('u128', 'u256', 'i128', 'i256')
    ORDER BY col.name
"

echo "round trip required, nullable, nested, and dictionary values"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        count(),
        min(u128), max(u128),
        min(u256), max(u256),
        min(i128), max(i128),
        min(i256), max(i256)
    FROM file('${DECIMAL_FILE}', Parquet, '${STRUCTURE}')
"
${CLICKHOUSE_LOCAL} --query="
    SELECT
        countIf(length(i128_array) = 1 AND i128_array[1] = i128),
        countIf(wide_tuple.1 = u256 AND wide_tuple.2 = i256),
        countIf(isNull(u128_nullable))
    FROM file('${DECIMAL_FILE}', Parquet, '${STRUCTURE}')
"
echo "dictionary filtering remains safely ineligible"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${DECIMAL_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 = toUInt128('340282366920938463463374607431768211455')
    SETTINGS
        input_format_parquet_filter_push_down = 0,
        input_format_parquet_page_filter_push_down = 0,
        input_format_parquet_bloom_filter_push_down = 0,
        input_format_parquet_dictionary_filter_push_down = 1048576
"

${CLICKHOUSE_LOCAL} --query="
    SELECT toUInt128(1) AS x
    SETTINGS output_format_parquet_wide_integer_as_decimal = 1
    FORMAT Parquet
" > "${INFER_39_FILE}"
echo "schema inference"
${CLICKHOUSE_LOCAL} --query="
    SELECT toTypeName(x) = 'Decimal(39, 0)'
    FROM file('${INFER_39_FILE}', Parquet)
    SETTINGS schema_inference_make_columns_nullable = 0
"

expect_rejected()
{
    local label="$1"
    local query="$2"
    local pattern="$3"
    local error
    echo "${label}"
    if error=$(${CLICKHOUSE_LOCAL} --query="${query}" 2>&1); then
        echo "unexpected success"
    elif grep -q "${pattern}" <<< "${error}"; then
        echo "rejected"
    else
        echo "unexpected error"
        echo "${error}"
    fi
}

expect_rejected "precision 78 inference requires an explicit structure" "
    SELECT u256 FROM file('${DECIMAL_FILE}', Parquet) FORMAT Null
" "precision 78 exceeds"

expect_rejected "precision 77 inference requires an explicit structure" "
    SELECT overflow FROM file('${INT256_OVERFLOW_FILE}', Parquet) FORMAT Null
" "precision 77 exceeds"

expect_rejected "negative decimal to UInt128" "
    SELECT i128 FROM file('${DECIMAL_FILE}', Parquet, 'i128 UInt128') FORMAT Null
" "Negative Parquet Decimal"

expect_rejected "UInt128 overflow to Int128" "
    SELECT u128 FROM file('${DECIMAL_FILE}', Parquet, 'u128 Int128') FORMAT Null
" "out of range"

expect_rejected "precision 78 cannot be represented as Int256" "
    SELECT u256 FROM file('${DECIMAL_FILE}', Parquet, 'u256 Int256') FORMAT Null
" "cannot be represented as Int256"

expect_rejected "precision 77 value overflows Int256 at runtime" "
    SELECT overflow FROM file('${INT256_OVERFLOW_FILE}', Parquet, 'overflow Int256') FORMAT Null
" "out of range"

expect_rejected "decimal width is too small for declared precision" "
    SELECT invalid FROM file('${INVALID_WIDTH_FILE}', Parquet, 'invalid UInt128') FORMAT Null
" "width 1 is too small for precision 3"

${CLICKHOUSE_LOCAL} --query="
    SELECT toDecimal256(1, 1) AS x FORMAT Parquet
" > "${SCALE_FILE}"
expect_rejected "nonzero-scale decimal to UInt256" "
    SELECT x FROM file('${SCALE_FILE}', Parquet, 'x UInt256') FORMAT Null
" "nonzero scale"

${CLICKHOUSE_LOCAL} --query="
    SELECT toUInt128('340282366920938463463374607431768211455') AS x
    SETTINGS
        output_format_parquet_wide_integer_as_decimal = 1,
        output_format_parquet_compression_method = 'none',
        output_format_parquet_max_dictionary_size = 0,
        output_format_parquet_write_checksums = 0,
        output_format_parquet_write_bloom_filter = 0
    FORMAT Parquet
" > "${CORRUPT_FILE}"
python3 - "${CORRUPT_FILE}" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = path.read_bytes()
valid = bytes.fromhex("00" + "ff" * 16)
malformed = bytes.fromhex("01" + "ff" * 16)
if valid not in data:
    raise RuntimeError("wide-integer decimal payload not found")
path.write_bytes(data.replace(valid, malformed))
PY
expect_rejected "malformed sign extension" "
    SELECT x FROM file('${CORRUPT_FILE}', Parquet, 'x UInt128') FORMAT Null
" "sign extension"

expect_rejected "incorrect standard-statistics width" "
    SELECT u128
    FROM file('${BAD_STATS_FILE}', Parquet, '${STRUCTURE}')
    WHERE u128 = toUInt128(1)
    SETTINGS input_format_parquet_filter_push_down = 1
    FORMAT Null
" "Unexpected value size"

# Four row groups with disjoint numeric ranges. The first nullable `UInt256` row group is all null.
${CLICKHOUSE_LOCAL} --query="
    SELECT
        multiIf(
            intDiv(number, 1000) = 0, toUInt128(number % 1000),
            intDiv(number, 1000) = 1, bitShiftLeft(toUInt128(1), 64) + toUInt128(number % 1000),
            intDiv(number, 1000) = 2, bitShiftLeft(toUInt128(1), 127) + toUInt128(number % 1000),
            toUInt128('340282366920938463463374607431768210456') + toUInt128(number % 1000)) AS u128,
        multiIf(
            intDiv(number, 1000) = 0, toUInt256(number % 1000),
            intDiv(number, 1000) = 1, bitShiftLeft(toUInt256(1), 128) + toUInt256(number % 1000),
            intDiv(number, 1000) = 2, bitShiftLeft(toUInt256(1), 255) + toUInt256(number % 1000),
            toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129638936') + toUInt256(number % 1000)) AS u256,
        if(number % 10 = 0, NULL, u128)::Nullable(UInt128) AS u128_nullable,
        if(intDiv(number, 1000) = 0, NULL, u256)::Nullable(UInt256) AS u256_nullable,
        number AS n
    FROM numbers(4000)
    SETTINGS
        output_format_parquet_wide_integer_as_decimal = 1,
        output_format_parquet_row_group_size = 1000,
        output_format_parquet_max_dictionary_size = 0,
        max_block_size = 1000000
    FORMAT Parquet
" > "${ROW_GROUP_FILE}"

ROW_GROUP_STRUCTURE="u128 UInt128, u256 UInt256, u128_nullable Nullable(UInt128), u256_nullable Nullable(UInt256), n UInt64"
PROFILE_ROW_GROUPS="${CLICKHOUSE_LOCAL} --input_format_parquet_filter_push_down=1 --input_format_parquet_page_filter_push_down=0 --input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

profile_row_groups()
{
    local query="$1"
    ${PROFILE_ROW_GROUPS} --print-profile-events --query="${query} FORMAT Null" 2>&1 | awk '
        /ParquetReadRowGroups:/   { read += $(NF-1) }
        /ParquetPrunedRowGroups:/ { pruned += $(NF-1) }
        END { print "read=" read+0 " pruned=" pruned+0 }
    '
}

echo "UInt128 row-group pruning"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(n), max(n)
    FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199
"
profile_row_groups "
    SELECT * FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u128 BETWEEN bitShiftLeft(toUInt128(1), 127) + 100 AND bitShiftLeft(toUInt128(1), 127) + 199"

echo "UInt256 row-group pruning"
${CLICKHOUSE_LOCAL} --query="
    SELECT count(), min(n), max(n)
    FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u256 BETWEEN bitShiftLeft(toUInt256(1), 255) + 100 AND bitShiftLeft(toUInt256(1), 255) + 199
"
profile_row_groups "
    SELECT * FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u256 BETWEEN bitShiftLeft(toUInt256(1), 255) + 100 AND bitShiftLeft(toUInt256(1), 255) + 199"

echo "nullable and all-null row-group pruning"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u128_nullable = bitShiftLeft(toUInt128(1), 127) + 101
"
${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u256_nullable = toUInt256(1)
"
profile_row_groups "
    SELECT * FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u256_nullable = toUInt256(1)"

echo "bloom-filter pushdown remains safely ineligible"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${ROW_GROUP_FILE}', Parquet, '${ROW_GROUP_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(101)
    SETTINGS
        input_format_parquet_filter_push_down = 0,
        input_format_parquet_page_filter_push_down = 0,
        input_format_parquet_bloom_filter_push_down = 1,
        input_format_parquet_dictionary_filter_push_down = 0
"

# One row group with 16 ordered pages. The last nullable page is all null.
${CLICKHOUSE_LOCAL} --query="
    SELECT
        bitShiftLeft(toUInt128(1), 127) + toUInt128(number) AS u128,
        bitShiftLeft(toUInt256(1), 255) + toUInt256(number) AS u256,
        if(number >= 3840, NULL, u256)::Nullable(UInt256) AS u256_nullable,
        number AS n
    FROM numbers(4096)
    SETTINGS
        output_format_parquet_wide_integer_as_decimal = 1,
        output_format_parquet_row_group_size = 100000,
        output_format_parquet_data_page_size = 4096,
        output_format_parquet_batch_size = 256,
        output_format_parquet_max_dictionary_size = 0,
        output_format_parquet_write_page_index = 1,
        max_block_size = 1000000
    FORMAT Parquet
" > "${PAGE_FILE}"

PAGE_STRUCTURE="u128 UInt128, u256 UInt256, u256_nullable Nullable(UInt256), n UInt64"

profile_pruned_pages()
{
    local query="$1"
    local profile
    if ! profile=$(${CLICKHOUSE_LOCAL} \
            --input_format_parquet_filter_push_down=0 \
            --input_format_parquet_page_filter_push_down=1 \
            --input_format_parquet_bloom_filter_push_down=0 \
            --input_format_parquet_dictionary_filter_push_down=0 \
            --input_format_parquet_use_offset_index=0 \
            --optimize_move_to_prewhere=0 \
            --use_cache_for_count_from_files=0 \
            --print-profile-events \
            --query="${query} FORMAT Null" 2>&1); then
        echo "${profile}" >&2
        return 1
    fi
    awk '
        /ParquetReadPages:/   { read += $(NF-1) }
        /ParquetPrunedPages:/ { pruned += $(NF-1) }
        END { print "read=" read+0 " pruned=" pruned+0 }
    ' <<< "${profile}"
}

echo "UInt128 page pruning"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)
"
profile_pruned_pages "
    SELECT u128 FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(2100)"

echo "UInt256 page pruning"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256 = bitShiftLeft(toUInt256(1), 255) + toUInt256(2100)
"
profile_pruned_pages "
    SELECT u256 FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256 = bitShiftLeft(toUInt256(1), 255) + toUInt256(2100)"

echo "nullable all-null page and predicate pruning every page"
${CLICKHOUSE_LOCAL} --query="
    SELECT groupArray(n)
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256_nullable = bitShiftLeft(toUInt256(1), 255) + toUInt256(300)
"
profile_pruned_pages "
    SELECT u256_nullable FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u256_nullable = bitShiftLeft(toUInt256(1), 255) + toUInt256(300)"
${CLICKHOUSE_LOCAL} --query="
    SELECT count()
    FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(10000)
"
profile_pruned_pages "
    SELECT u128 FROM file('${PAGE_FILE}', Parquet, '${PAGE_STRUCTURE}')
    WHERE u128 = bitShiftLeft(toUInt128(1), 127) + toUInt128(10000)"
