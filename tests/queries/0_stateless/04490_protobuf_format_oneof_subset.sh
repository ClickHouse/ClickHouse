#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Proto StringOrString has oneof { string1 = 1; string2 = 2; }.
# data_protobuf/String1 sets string1 (tag 1); data_protobuf/String2 sets string2 (tag 2).
# The presence Enum only needs to cover the oneof cases that have a backing column (plus the
# 'omitted' marker 0). This is the default behavior (only oneof_presence is enabled).
# This test intentionally uses direct field-like labels so it stays focused on numeric tag
# compatibility; label mismatches are already covered in `03447_protobuf_format_oneof.sh`.

# (a) Forward compat: the Enum and columns cover only tag 1. A message setting tag 2 has no
#     column and is not in the Enum, so it is ingested as omitted (presence = 'no'), tag-2 data
#     dropped. This is what would happen if the .proto later added the string2 case.
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_subset_additive_04490;
SELECT '>> additive_forward_compat';
CREATE TABLE oneof_subset_additive_04490 ( string1 String, string_oneof Enum('no'=0, 'string1'=1) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_subset_additive_04490 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
INSERT INTO oneof_subset_additive_04490 from INFILE '$CURDIR/data_protobuf/String2' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT string1, string_oneof FROM oneof_subset_additive_04490 ORDER BY string_oneof FORMAT TSV;
EOF

# (b) Dead values: the Enum may list values that no oneof case (and no column) produces.
#     Only 'ghost'=99 is dead here; ingestion still succeeds.
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_dead_values_04490;
SELECT '>> dead_value_tolerance';
CREATE TABLE oneof_dead_values_04490 ( string1 String, string2 String, string_oneof Enum('no'=0, 'string1'=1, 'string2'=2, 'ghost'=99) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_dead_values_04490 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
INSERT INTO oneof_dead_values_04490 from INFILE '$CURDIR/data_protobuf/String2' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT string1, string2, string_oneof FROM oneof_dead_values_04490 ORDER BY string1, string2 FORMAT TSV;
EOF

# (c) Rejected: tag 2 HAS a backing column (string2) but is missing from the Enum. Storing its
#     presence would write an out-of-range value into the Enum, so the schema is rejected.
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_poison_04490;
SELECT '>> rejected_column_tag_missing_from_enum';
CREATE TABLE oneof_poison_04490 ( string1 String, string2 String, string_oneof Enum('no'=0, 'string1'=1) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_poison_04490 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle; -- { clientError DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD }
EOF

# (d) Rejected: the Enum lacks the 'omitted' marker 0.
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_no_zero_04490;
SELECT '>> rejected_missing_omitted_marker';
CREATE TABLE oneof_no_zero_04490 ( string1 String, string_oneof Enum('string1'=1) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_no_zero_04490 from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/03447_string_or_string.proto:StringOrString' FORMAT ProtobufSingle; -- { clientError DATA_TYPE_INCOMPATIBLE_WITH_PROTOBUF_FIELD }
EOF

# (e) Same relaxation through a NESTED message oneof (Inner.string_oneof { string1=1, string2=2 }).
#     Only inner.string1 has a column and the Enum omits tag 2. InnerString1 -> presence 'string1';
#     InnerString2 sets inner.string2 (no column, not in Enum) -> presence 'no' (0).
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_nested_subset_04490;
SELECT '>> nested_additive_forward_compat';
CREATE TABLE oneof_nested_subset_04490 ( \`outer.string\` String, \`inner.string1\` String, \`inner.string.oneof\` Enum('no'=0, 'string1'=1) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_nested_subset_04490 from INFILE '$CURDIR/data_protobuf/InnerString1' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
INSERT INTO oneof_nested_subset_04490 from INFILE '$CURDIR/data_protobuf/InnerString2' SETTINGS format_schema='$SCHEMADIR/03447_inner_string_or_string.proto:InnerStringOrString' FORMAT ProtobufSingle;
SELECT \`outer.string\`, \`inner.string1\`, \`inner.string.oneof\` FROM oneof_nested_subset_04490 ORDER BY \`outer.string\` FORMAT TSV;
EOF

# (f) Same relaxation through a REPEATED oneof with an Array(Enum) presence column
#     (Item.value { int_value=2, string_value=3 }). Only items.int_value has a column and the
#     Enum omits tag 3. Per element: int_value -> 'int_value'; string_value (no column, not in
#     Enum) -> 'omitted' (0).
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_repeated_subset_04490;
SELECT '>> repeated_additive_forward_compat';
CREATE TABLE oneof_repeated_subset_04490 ( \`items.name\` Array(String), \`items.int_value\` Array(Int32), \`items.value\` Array(Enum8('omitted'=0, 'int_value'=2)) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_repeated_subset_04490 from INFILE '$CURDIR/data_protobuf/OneofRepeated' SETTINGS format_schema='$SCHEMADIR/03447_oneof_repeated.proto:TestOneOfRepeated' FORMAT ProtobufSingle;
SELECT \`items.name\`, \`items.int_value\`, \`items.value\` FROM oneof_repeated_subset_04490 FORMAT TSV;
EOF

# (g) Empty-message oneof branches should follow the same relaxed rule. Here the `.proto` has
#     two empty oneof cases { nothing=1, nothing2=2 }, but the Enum only lists tag 1. The input
#     message sets tag 2, which has no table-backed payload column, so ingestion must succeed and
#     the presence column falls back to 'unknown' (0) instead of rejecting serializer creation.
$CLICKHOUSE_CLIENT <<EOF
SET input_format_protobuf_oneof_presence=1;
DROP TABLE IF EXISTS oneof_empty_subset_04490;
SELECT '>> empty_additive_forward_compat';
CREATE TABLE oneof_empty_subset_04490 ( type Enum8('unknown'=0, 'nothing'=1) ) Engine=MergeTree ORDER BY tuple();
INSERT INTO oneof_empty_subset_04490 from INFILE '$CURDIR/data_protobuf/RecordTotallyEmpty' SETTINGS format_schema='$SCHEMADIR/04046_empty_record.proto:Record' FORMAT ProtobufSingle;
SELECT type FROM oneof_empty_subset_04490 FORMAT TSV;
EOF
