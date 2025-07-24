#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$(mktemp -q 02584_test_data_XXXXXX)
OUT_FILE=$(mktemp -q 02584_test_out_XXXXXX)

echo "Hello, World!" > $DATA_FILE;

$CLICKHOUSE_COMPRESSOR --codec 'Delta' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;
$CLICKHOUSE_COMPRESSOR --codec 'Delta(5)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Delta([1,2])' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Delta(4)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;

$CLICKHOUSE_COMPRESSOR --codec 'DoubleDelta' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;
$CLICKHOUSE_COMPRESSOR --codec 'DoubleDelta(5)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'DoubleDelta([1,2])' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'DoubleDelta(4)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;

$CLICKHOUSE_COMPRESSOR --codec 'Gorilla' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;
$CLICKHOUSE_COMPRESSOR --codec 'Gorilla(5)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Gorilla([1,2])' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Gorilla(4)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;

$CLICKHOUSE_COMPRESSOR --codec 'Chimp(1)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Chimp(5)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Chimp([1,2])' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'Chimp(4)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;

$CLICKHOUSE_COMPRESSOR --codec 'FPC' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;
$CLICKHOUSE_COMPRESSOR --codec 'FPC(5)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;
$CLICKHOUSE_COMPRESSOR --codec 'FPC(5, 1)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'FPC([1,2,3])' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "ILLEGAL_CODEC_PARAMETER";
$CLICKHOUSE_COMPRESSOR --codec 'FPC(5, 4)' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE;


$CLICKHOUSE_COMPRESSOR --codec 'T64' --codec 'LZ4' --input $DATA_FILE --output $OUT_FILE 2>&1 | grep -c "CANNOT_COMPRESS";

rm $DATA_FILE $OUT_FILE

