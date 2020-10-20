#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
DROP TABLE IF EXISTS in_persons_00825;
DROP TABLE IF EXISTS in_squares_00825;

CREATE TABLE in_persons_00825 (uuid UUID,
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
                               measureUnits Nested (unit String, coef Float32),
                               nestiness_a_b_c_d Nullable(UInt32),
                               `nestiness_a_B.c_E` Array(UInt32)
                              ) ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE in_squares_00825 (number UInt32, square UInt32) ENGINE = MergeTree ORDER BY tuple();
EOF

# To generate the file 00825_protobuf_format_input.insh use the following commands:
# ninja ProtobufDelimitedMessagesSerializer
# build/utils/test-data-generator/ProtobufDelimitedMessagesSerializer
source "$CURDIR"/00825_protobuf_format_input.insh

$CLICKHOUSE_CLIENT --query "SELECT * FROM in_persons_00825 ORDER BY uuid;"
$CLICKHOUSE_CLIENT --query "SELECT * FROM in_squares_00825 ORDER BY number;"

$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE in_persons_00825;"
$CLICKHOUSE_CLIENT --query "TRUNCATE TABLE in_squares_00825;"

source "$CURDIR"/00825_protobuf_format_input_single.insh

$CLICKHOUSE_CLIENT --query "SELECT * FROM in_persons_00825 ORDER BY uuid;"
$CLICKHOUSE_CLIENT --query "SELECT * FROM in_squares_00825 ORDER BY number;"

# Try to input malformed data.
set +eo pipefail
echo -ne '\xe0\x80\x3f\x0b' \
    | $CLICKHOUSE_CLIENT --query="INSERT INTO in_persons_00825 FORMAT Protobuf SETTINGS format_schema = '$CURDIR/00825_protobuf_format:Person'" 2>&1 \
    | grep -qF "Protobuf messages are corrupted" && echo "ok" || echo "fail"
set -eo pipefail

# Try to input malformed data for ProtobufSingle
set +eo pipefail
echo -ne '\xff\xff\x3f\x0b' \
    | $CLICKHOUSE_CLIENT --query="INSERT INTO in_persons_00825 FORMAT ProtobufSingle SETTINGS format_schema = '$CURDIR/00825_protobuf_format:Person'" 2>&1 \
    | grep -qF "Protobuf messages are corrupted" && echo "ok" || echo "fail"
set -eo pipefail

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS in_persons_00825;"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS in_squares_00825;"
