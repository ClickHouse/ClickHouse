#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..

DATA_FILE=$CUR_DIR/data_arrow/test.arrow

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_load (bool UInt8, int8 Int8, int16 Int16, int32 Int32, int64 Int64, uint8 UInt8, uint16 UInt16, uint32 UInt32, uint64 UInt64, halffloat Float32, float Float32, double Float64, string String, date32 Date, date64 DateTime, timestamp DateTime) ENGINE = Memory"
cat "$DATA_FILE"  | ${CLICKHOUSE_CLIENT} -q "insert into arrow_load format Arrow"
${CLICKHOUSE_CLIENT} --query="select * from arrow_load"

$CLICKHOUSE_CLIENT -q "DROP TABLE arrow_load"
