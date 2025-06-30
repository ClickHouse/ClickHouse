#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS string_or_string_3447;
SELECT '>> string_or_string';
CREATE TABLE string_or_string_3447 ( string1 String, string2 String, string_oneof_presence Enum('no'=0, 'hello' = 1, 'world' = 2))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
INSERT INTO string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String2' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string_3447 ORDER BY string1 Format pretty;
EOF

$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS inner_string_or_string_3447;
SELECT '>> inner_string_or_string';
CREATE TABLE inner_string_or_string_3447 ( outer_string String, inner_string1 String, inner_string2 String, inner_string_oneof_presence Enum('no'=0, 'hello' = 1, 'world' = 2))  Engine=MergeTree ORDER BY tuple();
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/InnerString1' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/InnerString2' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
INSERT INTO inner_string_or_string_3447 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
SELECT * FROM inner_string_or_string_3447 ORDER BY outer_string Format pretty;
EOF

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
    payment_details_presence Enum8('ommited' = 0, 'buy' = 2, 'sell' = 3),
    buy_payment_value_presence Enum8('ommited' = 0, 'cash_value' = 1, 'card_value' = 2),
    sell_payment_value_presence Enum8('ommited' = 0, 'cash_value' = 1, 'card_value' = 2),
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/tbuy' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/tsell' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
INSERT INTO transaction_3447 from INFILE '$CURDIR/data_protobuf/temptysell' SETTINGS format_schema='$SCHEMADIR/03447_oneof_transaction.proto:Transaction' FORMAT ProtobufSingle;
SELECT * FROM transaction_3447 ORDER BY date, buy_payment_cash_value Format Vertical;
EOF
