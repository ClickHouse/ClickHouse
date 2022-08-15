#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS splitted_nested_protobuf_00825;

CREATE TABLE splitted_nested_protobuf_00825 (
  a String,
  b Int64,
  c Int32,
  d Int32, 
  e String, 
  f String, 
  g String, 
  h String, 
  i Int32, 
  j Int32, 
  k Nullable(Int32), 
  l Nullable(Int32), 
  m String, 
  sub_1_a Nullable(Int32), 
  sub_1_b Nullable(Int32), 
  sub_1_c Nullable(String), 
  sub_1_d Nullable(String), 
  sub_1_e Nullable(String), 
  sub_1_f Nullable(String), 
  sub_1_g Nullable(String), 
  sub_1_h Nullable(String), 
  sub_1_i Nullable(String), 
  sub_1_j Nullable(String), 
  sub_1_k Nullable(String), 
  sub_2_a Nullable(Int32),
  sub_2_b Nullable(String), 
  sub_2_c Nullable(Int32), 
  sub_2_d Nullable(Int32), 
  sub_2_e Nullable(Int32), 
  sub_2_f Nullable(Int32), 
  sub_2_g Nullable(String), 
  sub_2_h Nullable(String), 
  sub_2_i Nullable(String), 
  sub_2_j Nullable(String), 
  sub_2_k Nullable(Int64), 
  sub_2_l Nullable(Int64), 
  n Int32, 
  sub_2_random_name Nullable(String)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO splitted_nested_protobuf_00825 VALUES ('tags for first fixed value', 1622559733, 920, 1, '79034445678', '250208889765444', '35655678903421', '79991232222', 250, 20, 18122, 22010, 'text for the first fixed value', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3, '172.18.20.11', 47855, 32705, 26855, 51940, '0x1dbb09', '_49597', 'msc_number_52317', '0x750x830xa50xb', 31453, 49538, 1, '522d');

SELECT * FROM splitted_nested_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_splitted_nested.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM splitted_nested_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_splitted_nested:Some'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_splitted_nested:some.Some" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO splitted_nested_protobuf_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_splitted_nested:Some'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM splitted_nested_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP TABLE splitted_nested_protobuf_00825"
