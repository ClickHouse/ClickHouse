#!/usr/bin/env bash

#
# Load all possible .parquet files found in submodules.
# TODO: Add more files.
#

# To regenerate data install perl JSON::XS module: sudo apt install libjson-xs-perl

# Also 5 sample files from
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata1.parquet
# ...
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata5.parquet


# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ "$CB_DIR" != "." ] && BUILD_DIR=$CB_DIR/../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..

DATA_DIR=$CUR_DIR/data_parquet

# To update:
# cp $ROOT_DIR/contrib/arrow/cpp/submodules/parquet-testing/data/*.parquet $ROOT_DIR/contrib/arrow/python/pyarrow/tests/data/parquet/*.parquet $CUR_DIR/data_parquet/

# BUG! nulls.snappy.parquet - parquet-reader shows wrong structure. Actual structure is {"type":"struct","fields":[{"name":"b_struct","type":{"type":"struct","fields":[{"name":"b_c_int","type":"integer","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}
# why? repeated_no_annotation.parquet

for NAME in $(find "$DATA_DIR"/*.parquet -print0 | xargs -0 -n 1 basename | sort); do
    echo === Try load data from "$NAME"

    JSON=$DATA_DIR/$NAME.json
    COLUMNS_FILE=$DATA_DIR/$NAME.columns

    # If you want change or add .parquet file - rm data_parquet/*.json data_parquet/*.columns
    [ -n "$BUILD_DIR" ] && [ ! -s "$COLUMNS_FILE" ] && [ ! -s "$JSON" ] && "$BUILD_DIR"/contrib/arrow-cmake/parquet-reader --json "$DATA_DIR"/"$NAME" > "$JSON"
    [ -n "$BUILD_DIR" ] && [ ! -s "$COLUMNS_FILE" ] && "$CUR_DIR"/00900_parquet_create_table_columns.pl "$JSON" > "$COLUMNS_FILE"

    # Debug only:
    # [ -n "$BUILD_DIR" ] && $BUILD_DIR/contrib/arrow-cmake/parquet-reader $DATA_DIR/$NAME > $DATA_DIR/$NAME.dump

    #COLUMNS=`$CUR_DIR/00900_parquet_create_table_columns.pl $JSON` 2>&1 || continue
    COLUMNS=$(cat "$COLUMNS_FILE") || continue

    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
    ${CLICKHOUSE_CLIENT} --query="CREATE TABLE parquet_load ($COLUMNS) ENGINE = Memory"

    # Some files is broken, exception is ok.
    cat "$DATA_DIR"/"$NAME" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_load FORMAT Parquet" 2>&1 | sed 's/Exception/Ex---tion/'

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load LIMIT 100"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_load"
done
