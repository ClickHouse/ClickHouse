#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# syntax = "proto3";
# message Empty {}
# message Clear {}
# message Info {
#   string name = 1;
#   int32 age = 2;
# }
# message Record {
#   string id = 1;
#   oneof type {
#     Info details = 3;
#     Empty nothing = 4;
#     Empty nothing2 = 5;
#     Clear nothing3 = 6;
#   }
# }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS records_04046;
SELECT '>> records';
CREATE TABLE records_04046
(
    id String,
    type Enum8('unknown' = 0, 'details' = 3, 'nothing' = 4, 'nothing2' = 5, 'nothing3' = 6),
    details Tuple(
        name Nullable(String),
        age Nullable(Int32))
)
ENGINE = MergeTree;
INSERT INTO records_04046 from INFILE '$CURDIR/data_protobuf/RecordEmpty' SETTINGS format_schema='$SCHEMADIR/04046_record.proto:Record' FORMAT ProtobufSingle;
INSERT INTO records_04046 from INFILE '$CURDIR/data_protobuf/RecordInfo' SETTINGS format_schema='$SCHEMADIR/04046_record.proto:Record' FORMAT ProtobufSingle;
INSERT INTO records_04046 from INFILE '$CURDIR/data_protobuf/RecordClear' SETTINGS format_schema='$SCHEMADIR/04046_record.proto:Record' FORMAT ProtobufSingle;
SELECT * FROM records_04046 ORDER BY id Format PrettyMonoBlock;
DROP TABLE records_04046;
EOF

# syntax = "proto3";
# message Empty {}
# message Record {
#   oneof type {
#     Empty nothing = 1;
#     Empty nothing2 = 2;
#   }
# }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS empty_records_04046;
SELECT '>> empty records';
CREATE TABLE empty_records_04046
(
    type Enum8('unknown' = 0, 'nothing' = 1, 'nothing2' = 2)
)
ENGINE = MergeTree;
INSERT INTO empty_records_04046 from INFILE '$CURDIR/data_protobuf/RecordTotallyEmpty' SETTINGS format_schema='$SCHEMADIR/04046_empty_record.proto:Record' FORMAT ProtobufSingle;
SELECT * FROM empty_records_04046 ORDER BY type Format PrettyMonoBlock;
DROP TABLE empty_records_04046;
EOF

# syntax = "proto3";
# message Empty {}
# message InnerRecord {
#   oneof type {
#     Empty nothing = 1;
#     Empty nothing2 = 2;
#   }
# }
# message Record {
#     int32 id = 1;
# 		InnerRecord inner = 2;
# }
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=true;
DROP TABLE IF EXISTS empty_inner_records_04046;
SELECT '>> empty inner records';
CREATE TABLE empty_inner_records_04046
(
		id Int32,
    inner_type Enum8('unknown' = 0, 'nothing' = 1, 'nothing2' = 2)
)
ENGINE = MergeTree;
INSERT INTO empty_inner_records_04046 from INFILE '$CURDIR/data_protobuf/RecordInnerEmpty' SETTINGS format_schema='$SCHEMADIR/04046_inner_record.proto:Record' FORMAT ProtobufSingle;
SELECT * FROM empty_inner_records_04046 ORDER BY id Format PrettyMonoBlock;
DROP TABLE empty_inner_records_04046;
EOF
