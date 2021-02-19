#!/usr/bin/env bash

# To generate reference file for this test use the following commands:
# ninja ProtobufDelimitedMessagesSerializer
# build/utils/test-data-generator/ProtobufDelimitedMessagesSerializer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE IF EXISTS persons_00825;
DROP TABLE IF EXISTS roundtrip_persons_00825;
DROP TABLE IF EXISTS alt_persons_00825;
DROP TABLE IF EXISTS str_persons_00825;
DROP TABLE IF EXISTS syntax2_persons_00825;

CREATE TABLE persons_00825 (uuid UUID,
                            name String,
                            surname String,
                            gender Enum8('male'=1, 'female'=0),
                            birthDate Date,
                            photo Nullable(String),
                            phoneNumber Nullable(FixedString(13)),
                            isOnline UInt8,
                            visitTime Nullable(DateTime),
                            age UInt8,
                            zodiacSign Enum16('aries'=321, 'taurus'=420, 'gemini'=521, 'cancer'=621, 'leo'=723, 'virgo'=823,
                                              'libra'=923, 'scorpius'=1023, 'sagittarius'=1122, 'capricorn'=1222, 'aquarius'=120,
                                              'pisces'=219),
                            songs Array(String),
                            color Array(UInt8),
                            hometown LowCardinality(String),
                            location Array(Decimal32(6)),
                            pi Nullable(Float64),
                            lotteryWin Nullable(Decimal64(2)),
                            someRatio Float32,
                            temperature Decimal32(1),
                            randomBigNumber Int64,
                            measureUnits Nested(unit  String, coef Float32),
                            nestiness_a_b_c_d Nullable(UInt32),
                            \`nestiness_a_B.c_E\` Array(UInt32)
                           ) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO persons_00825 VALUES (toUUID('a7522158-3d41-4b77-ad69-6c598ee55c49'), 'Ivan', 'Petrov', 'male', toDate('1980-12-29'), 'png', '+74951234567', 1, toDateTime('2019-01-05 18:45:00'), 38, 'capricorn', ['Yesterday', 'Flowers'], [255, 0, 0], 'Moscow', [55.753215, 37.622504], 3.14, 214.10, 0.1, 5.8, 17060000000, ['meter', 'centimeter', 'kilometer'], [1, 0.01, 1000], 500, [501, 502]);
INSERT INTO persons_00825 VALUES (toUUID('c694ad8a-f714-4ea3-907d-fd54fb25d9b5'), 'Natalia', 'Sokolova', 'female', toDate('1992-03-08'), 'jpg', NULL, 0, NULL, 26, 'pisces', [], [100, 200, 50], 'Plymouth', [50.403724, -4.142123], 3.14159, NULL, 0.007, 5.4, -20000000000000, [], [], NULL, []);
INSERT INTO persons_00825 VALUES (toUUID('a7da1aa6-f425-4789-8947-b034786ed374'), 'Vasily', 'Sidorov', 'male', toDate('1995-07-28'), 'bmp', '+442012345678', 1, toDateTime('2018-12-30 00:00:00'), 23, 'leo', ['Sunny'], [250, 244, 10], 'Murmansk', [68.970682, 33.074981], 3.14159265358979, 100000000000, 800, -3.2, 154400000, ['pound'], [16], 503, []);

SELECT * FROM persons_00825 ORDER BY name;
EOF

# Use schema 00825_protobuf_format_persons:Person
echo
echo "Schema 00825_protobuf_format_persons:Person"
BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_00825 ORDER BY name FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_persons:Person'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_persons:Person" --input "$BINARY_FILE_PATH"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE roundtrip_persons_00825 AS persons_00825"
$CLICKHOUSE_CLIENT --query "INSERT INTO roundtrip_persons_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_persons:Person'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM roundtrip_persons_00825 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 00825_protobuf_format_persons:AltPerson
echo
echo "Schema 00825_protobuf_format_persons:AltPerson"
BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_00825 ORDER BY name FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_persons:AltPerson'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_persons:AltPerson" --input "$BINARY_FILE_PATH"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE alt_persons_00825 AS persons_00825"
$CLICKHOUSE_CLIENT --query "INSERT INTO alt_persons_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_persons:AltPerson'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM alt_persons_00825 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 00825_protobuf_format_persons:StrPerson
echo
echo "Schema 00825_protobuf_format_persons:StrPerson"
BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_00825 ORDER BY name FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_persons:StrPerson'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_persons:StrPerson" --input "$BINARY_FILE_PATH"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE str_persons_00825 AS persons_00825"
$CLICKHOUSE_CLIENT --query "INSERT INTO str_persons_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_persons:StrPerson'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM str_persons_00825 ORDER BY name"
rm "$BINARY_FILE_PATH"

# Use schema 00825_protobuf_format_syntax2:Syntax2Person
echo
echo "Schema 00825_protobuf_format_syntax2:Syntax2Person"
BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_persons.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM persons_00825 ORDER BY name FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_persons_syntax2:Syntax2Person'" > $BINARY_FILE_PATH
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_persons_syntax2:Syntax2Person" --input "$BINARY_FILE_PATH"
echo
echo "Roundtrip:"
$CLICKHOUSE_CLIENT --query "CREATE TABLE syntax2_persons_00825 AS persons_00825"
$CLICKHOUSE_CLIENT --query "INSERT INTO syntax2_persons_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_persons_syntax2:Syntax2Person'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM syntax2_persons_00825 ORDER BY name"
rm "$BINARY_FILE_PATH"

$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP TABLE persons_00825;
DROP TABLE roundtrip_persons_00825;
DROP TABLE alt_persons_00825;
DROP TABLE str_persons_00825;
DROP TABLE syntax2_persons_00825;
EOF
