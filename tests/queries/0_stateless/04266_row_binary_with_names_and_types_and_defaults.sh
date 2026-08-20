#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Case 1: single column, marker=0x01 -> default 42
# Header: 1 column named "x" of type "UInt32"
#   VarUInt 1               -> 01
#   "x" (len 1)              -> 01 78
#   type name "UInt32" (len 6) -> 06 55 49 6e 74 33 32
# Row: one cell with marker=0x01 (use default)
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x UInt32 DEFAULT 42',
    unhex('01' || '0178' || '06' || hex('UInt32') || '01')
);"

# Case 2: single column, marker=0x00 + UInt32 value 7
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x UInt32 DEFAULT 42',
    unhex('01' || '0178' || '06' || hex('UInt32') || '00' || '07000000')
);"

# Case 3a: Nullable(UInt32) DEFAULT 42, cell=0x01 -> 42
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);"

# Case 3b: Nullable(UInt32) DEFAULT 42, cell=0x00 0x01 -> NULL
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '00' || '01')
);"

# Case 3c: Nullable(UInt32) DEFAULT 42, cell=0x00 0x00 + value 5
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '00' || '00' || '05000000')
);"

# Case 4: two columns, mix per-column markers
# Schema: a UInt32 DEFAULT 1, b UInt32 DEFAULT 2
# Row: a marker=0x01 (default 1) ; b marker=0x00 value=9
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'a UInt32 DEFAULT 1, b UInt32 DEFAULT 2',
    unhex('02' || '0161' || '0162' || '06' || hex('UInt32') || '06' || hex('UInt32') || '01' || '00' || '09000000')
);"

# Case 5: same as Case 4 but with binary type encoding for the header
# UInt32 in binary type encoding is 0x03 (BinaryTypeIndex::UInt32 = 0x03)
$CLICKHOUSE_LOCAL --query "
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'a UInt32 DEFAULT 1, b UInt32 DEFAULT 2',
    unhex('02' || '0161' || '0162' || '03' || '03' || '01' || '00' || '09000000')
) SETTINGS input_format_binary_decode_types_in_binary_format = 1;"

# Case 6: schema inference via DESCRIBE — only the header is consumed
$CLICKHOUSE_LOCAL --query "
DESCRIBE TABLE format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    unhex('02' || '0161' || '0162' || '06' || hex('UInt32') || '10' || hex('Nullable(String)'))
) FORMAT TSVRaw;"
