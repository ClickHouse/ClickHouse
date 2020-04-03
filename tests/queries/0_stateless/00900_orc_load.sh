#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ "$CB_DIR" != "." ] && BUILD_DIR=$CB_DIR/../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..

DATA_FILE=$CUR_DIR/data_orc/test.orc

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS orc_load"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE orc_load (int Int32, smallint Int8, bigint Int64, float Float32, double Float64, date Date, y String) ENGINE = Memory"
cat $DATA_FILE  | ${CLICKHOUSE_CLIENT} -q "insert into orc_load format ORC"
${CLICKHOUSE_CLIENT} --query="select * from orc_load"

