#!/usr/bin/env bash

#
# Load all possible .parquet files found in submodules.
# TODO: Add more files.
#

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CUR_DIR/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../../..
[ "$CB_DIR" != "." ] && BUILD_DIR=$CB_DIR/../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../../..
DATA_DIR=$CUR_DIR/data

# TODO: copy data files via cmake to build_dir (and pack to package)

# BUG! nulls.snappy.parquet
# why? repeated_no_annotation.parquet

for DATA_SOURCE_DIR in $ROOT_DIR/contrib/arrow/cpp/submodules/parquet-testing/data $ROOT_DIR/contrib/arrow/python/pyarrow/tests/data/parquet; do
  for NAME in `ls -1 $DATA_SOURCE_DIR/*.parquet | xargs -n 1 basename | sort`; do
    echo === Try load data from $NAME

    [ ! -f "$DATA_DIR/$NAME" ] && DATA_DIR=$DATA_SOURCE_DIR
    JSON=$DATA_DIR/$NAME.json

    # Debug only:
    [ -n "$BUILD_DIR" ] && $BUILD_DIR/contrib/arrow-cmake/parquet-reader --json $DATA_DIR/$NAME > $JSON
    [ -n "$BUILD_DIR" ] && $BUILD_DIR/contrib/arrow-cmake/parquet-reader $DATA_DIR/$NAME > $DATA_DIR/$NAME.dump

    COLUMNS=`$CUR_DIR/00900_parquet_create_table_columns.pl $JSON` 2>&1 || continue

    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.parquet_load"
    ${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.parquet_load ($COLUMNS) ENGINE = Memory"

    # Some files is broken, exception is ok.
    cat $DATA_DIR/$NAME | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.parquet_load FORMAT Parquet" 2>&1 | sed 's/Exception/Ex---tion/'

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.parquet_load"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE test.parquet_load"
  done
done
