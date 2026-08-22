#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# string1: "string1"
# string2: "string2"
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS string_or_string_3447;
SELECT '>> string_or_string';
CREATE TABLE string_or_string_3447 ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 2) )  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
INSERT INTO string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String2' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string_3447 ORDER BY string1 Format PrettyMonoBlock;
EOF


# outer_string: "outer_string1"
# inner {
#   string1: "string1"
# }

# outer_string: "outer_string2"
# inner {
#   string2: "string2"
# }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS inner_string_or_string_3447;
SELECT '>> inner_string_or_string';
CREATE TABLE inner_string_or_string_3447 ( \`outer.string\` String, \`inner.string1\` String, \`inner.string15_not_used\` String, \`inner.string2\` String, \`inner.string.oneof\` Enum('no'=0, 'hello' = 1, 'world' = 2) )  Engine=MergeTree ORDER BY tuple();
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/InnerString1' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/InnerString2' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
SELECT * FROM inner_string_or_string_3447 ORDER BY \`outer.string\` Format PrettyMonoBlock;
EOF

# date: "10.10.2025"
# buy {
#   payment {
#     cash_value: 10.0
#   }
#   vendor_name: "dell"
#   items_bought: 1
# }

# date: "10.10.2025"
# sell {
#   payment {
#     cash_value: 10.0
#   }
#   customer_name: "bas&friends"
#   items_sold: 2
# }

# date: "2024-06-01"
# sell {
# }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS transaction_3447;
SELECT '>> oneof_transaction';
CREATE TABLE transaction_3447
(
    date String,
    buy_payment_cash_value Float64,
    buy_payment_card_value Float64,
    buy_vendor_name String,
    buy_items_bought Int32,
    sell_payment_cash_value Float64,
    sell_payment_card_value Float64,
    sell_customer_name String,
    sell_items_sold Int32,
    payment_details Enum8('omitted' = 0, 'buy' = 2, 'sell' = 3),
    buy_payment_value Enum8('omitted' = 0, 'cash_value' = 1, 'card_value' = 2),
    sell_payment_value Enum8('omitted' = 0, 'cash_value' = 1, 'card_value' = 2),
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/tbuy' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/tsell' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/temptysell' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
SELECT * FROM transaction_3447 ORDER BY date, buy_payment_cash_value Format Vertical;
EOF

# items: { name: "item1" int_value: 10 }
# items: { name: "item2" string_value: "foo" }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS oneof_repeated_3447;
SELECT '>> oneof_repeated';
CREATE TABLE oneof_repeated_3447
(
    \`items.name\` Array(String),
    \`items.int_value\` Array(Int32),
    \`items.string_value\` Array(String),
    \`items.value\` Array(Enum8('omitted' = 0, 'int_value' = 2, 'string_value' = 3))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO oneof_repeated_3447 from INFILE '$CURDIR/data_protobuf/OneofRepeated' SETTINGS format_schema='$SCHEMADIR/03447_oneof_repeated.proto:TestOneOfRepeated' FORMAT ProtobufSingle;
SELECT * FROM oneof_repeated_3447 ORDER BY \`items.name\` FORMAT Vertical;
EOF

# string1: "string1"
# string2: "string2"
# string_oneof column does not contain tag 2
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS string_or_string_exception_enum_3447;
SELECT '>> string_or_string_exception_enum';
CREATE TABLE string_or_string_exception_enum_3447 ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42) )  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string_exception_enum_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle; -- { clientError DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD }
EOF

# string1: "string1"
# string2: "string2"
# string_oneof column is float, which is inappropriate
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS string_or_string_exception_float_3447;
SELECT '>> string_or_string_exception_float';
CREATE TABLE string_or_string_exception_float_3447 ( string1 String, string2 String, string_oneof Float )  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string_exception_float_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle; -- { clientError DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD }
EOF

# string1: "string1"
# string2: "string2"
# string_oneof column is Int32
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS string_or_string_int32_3447;
SELECT '>> string_or_string_int32';
CREATE TABLE string_or_string_int32_3447 ( string1 String, string2 String, string_oneof Int32 )  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string_int32_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string_int32_3447 ORDER BY string1 FORMAT Vertical;
EOF
