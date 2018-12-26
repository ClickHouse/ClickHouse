#!/usr/bin/env bash

set -e
#set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ "$CB_DIR" != "." ] && BUILD_DIR=$CB_DIR/../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..
DATA_DIR=$ROOT_DIR/contrib/arrow/cpp/submodules/parquet-testing/data/

# alltypes_dictionary.parquet
# alltypes_plain.parquet
# alltypes_plain.snappy.parquet
# byte_array_decimal.parquet
# datapage_v2.snappy.parquet
# fixed_length_decimal_legacy.parquet
# fixed_length_decimal.parquet
# int32_decimal.parquet
# int64_decimal.parquet
# nation.dict-malformed.parquet
# nested_lists.snappy.parquet
# nested_maps.snappy.parquet
# nonnullable.impala.parquet
# nullable.impala.parquet
# nulls.snappy.parquet
# repeated_no_annotation.parquet

# byte_array_decimal.parquet
for NAME in alltypes_dictionary.parquet alltypes_plain.parquet nation.dict-malformed.parquet; do 
    #echo $DATA_DIR / $NAME
    echo $NAME
    JSON=$DATA_DIR/$NAME.json
    [ -n "$BUILD_DIR" ] && $BUILD_DIR/contrib/arrow-cmake/parquet-reader --json $DATA_DIR/$NAME > $JSON
    [ -n "$BUILD_DIR" ] && $BUILD_DIR/contrib/arrow-cmake/parquet-reader $DATA_DIR/$NAME > $DATA_DIR/$NAME.dump

    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_load"
    COLUMNS=`$CUR_DIR/00900_parquet_create_table_columns.pl $JSON`
    ${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_load ($COLUMNS) ENGINE = Memory"
    cat $DATA_DIR/$NAME | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_load FORMAT Parquet"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_load"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_load"

done
