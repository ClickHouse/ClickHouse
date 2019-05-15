#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
DROP TABLE IF EXISTS table_00825;

CREATE TABLE table_00825 (uuid UUID,
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
EOF

# To generate the file 00825_protobuf_format_input.insh use the following commands:
# ninja ProtobufDelimitedMessagesSerializer
# build/utils/test-data-generator/ProtobufDelimitedMessagesSerializer
source $CURDIR/00825_protobuf_format_input.insh

$CLICKHOUSE_CLIENT --query "SELECT * FROM table_00825 ORDER BY uuid;"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS table_00825;"
