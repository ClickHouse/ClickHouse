#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: MsgPack and Protobuf formats are not available in the fast test build.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# max_execution_time is a safety belt so a regression fails fast instead of hanging the suite. The
# assertion is always the error code or the row count, never the elapsed time.
BOUND="SETTINGS max_execution_time = 30"

DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$DIR"
mkdir -p "$DIR"

PB_STRUCT="a UInt64, b Int32, c Float64, d String"

$CLICKHOUSE_LOCAL -q "
    SELECT number AS a, toInt32(number) AS b, toFloat64(number) AS c, toString(number) AS d
    FROM numbers(777)
    INTO OUTFILE '$DIR/ok.pb' TRUNCATE FORMAT ProtobufList"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS c1, number * 2 AS c2 FROM numbers(555)
    INTO OUTFILE '$DIR/ok.msgpk' TRUNCATE FORMAT MsgPack"

# A ProtobufList envelope whose payload ends mid-message: readFieldNumber reports the end of the
# message while the underlying buffer still has bytes, so the count loop made no progress.
printf '\x00\x00' > "$DIR/bad.pb"
# 0xc1 is the one byte value MessagePack never assigns, so the parser reports a parse error without
# consuming it.
printf '\xc1' > "$DIR/bad.msgpk"
# Three well formed objects read back with a two column schema, so the last row is missing its second
# value: the object count is not a multiple of the column count.
$CLICKHOUSE_LOCAL -q "
    SELECT number AS c1 FROM numbers(3)
    INTO OUTFILE '$DIR/odd.msgpk' TRUNCATE FORMAT MsgPack"

echo '--- corrupted input is rejected instead of counted'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/bad.pb', ProtobufList, '$PB_STRUCT')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Unexpected end of ProtobufList message'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/bad.msgpk', MsgPack, 'c1 UInt64')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Error occurred while parsing msgpack data'
    $CLICKHOUSE_LOCAL -q "
        SELECT count() FROM file('$DIR/odd.msgpk', MsgPack, 'c1 UInt64, c2 UInt64')
        $BOUND, optimize_count_from_files = $setting" 2>&1 | grep -c -F 'Not enough values to complete the row'
done

echo '--- valid input still counts every row'
for setting in 1 0; do
    echo "optimize_count_from_files = $setting"
    for args in "'$DIR/ok.pb', ProtobufList, '$PB_STRUCT'" \
                "'$DIR/ok.msgpk', MsgPack, 'c1 UInt64, c2 UInt64'"; do
        $CLICKHOUSE_LOCAL -q "SELECT count() FROM file($args) $BOUND, optimize_count_from_files = $setting"
    done
done

echo '--- an empty and a single byte ProtobufList input are still valid and empty'
: > "$DIR/empty.pb"
printf '\x00' > "$DIR/one.pb"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/empty.pb', ProtobufList, '$PB_STRUCT') $BOUND"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DIR/one.pb', ProtobufList, '$PB_STRUCT') $BOUND"

echo '--- reading valid input is unchanged, at several block sizes'
for size in 1 13 65505; do
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(a, b, c, d))
        FROM file('$DIR/ok.pb', ProtobufList, '$PB_STRUCT')
        $BOUND, max_block_size = $size"
    $CLICKHOUSE_LOCAL -q "
        SELECT count(), sum(cityHash64(c1, c2))
        FROM file('$DIR/ok.msgpk', MsgPack, 'c1 UInt64, c2 UInt64')
        $BOUND, max_block_size = $size"
done

rm -rf "$DIR"
