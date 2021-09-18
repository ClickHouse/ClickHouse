#!/usr/bin/env bash
# Tags: long, no-unbundled, no-fasttest

#
# Load all possible .parquet files found in submodules.
# TODO: Add more files.
#

# Also 5 sample files from
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata1.parquet
# ...
# wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata5.parquet


# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CB_DIR=$(dirname "$CLICKHOUSE_CLIENT_BINARY")
[ "$CB_DIR" == "." ] && ROOT_DIR=$CUR_DIR/../../..
[ -z "$ROOT_DIR" ] && ROOT_DIR=$CB_DIR/../..

DATA_DIR=$CUR_DIR/data_parquet

[ -n "$ROOT_DIR" ] && [ -z "$PARQUET_READER" ] && PARQUET_READER="$ROOT_DIR"/contrib/arrow/cpp/build/release/parquet-reader

# To update:
# cp $ROOT_DIR/contrib/arrow/cpp/submodules/parquet-testing/data/*.parquet $ROOT_DIR/contrib/arrow/python/pyarrow/tests/data/parquet/*.parquet $CUR_DIR/data_parquet/

# ClickHouse Parquet reader doesn't support such complex types, so I didn't burrow into the issue.
# There is failure due parsing nested arrays or nested maps with NULLs:
# ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:192:  Check failed: (self->list_type_->value_type()->id()) == (data->child_data[0]->type->id())

# Strange behaviour for repeated_no_annotation.parquet around __buitin_expect, so this file was disabled:
# debug:
#   ../contrib/arrow/cpp/src/arrow/array/array_nested.cc:193:  Check failed: self->list_type_->value_type()->Equals(data->child_data[0]->type)
# release:
#   Code: 349. DB::Ex---tion: Can not insert NULL data into non-nullable column "phoneNumbers": data for INSERT was parsed from stdin

for NAME in $(find "$DATA_DIR"/*.parquet -print0 | xargs -0 -n 1 basename | LC_ALL=C sort); do
    echo === Try load data from "$NAME"

    JSON=$DATA_DIR/$NAME.json
    COLUMNS_FILE=$DATA_DIR/$NAME.columns

    # If you want change or add .parquet file - rm data_parquet/*.json data_parquet/*.columns
    [ -n "$PARQUET_READER" ] && [ ! -s "$COLUMNS_FILE" ] && [ ! -s "$JSON" ] && "$PARQUET_READER" --json "$DATA_DIR"/"$NAME" > "$JSON"
    [ ! -s "$COLUMNS_FILE" ] && "$CUR_DIR"/helpers/00900_parquet_create_table_columns.py "$JSON" > "$COLUMNS_FILE"

    # Debug only:
    # [ -n "$PARQUET_READER" ] && $PARQUET_READER $DATA_DIR/$NAME > $DATA_DIR/$NAME.dump

    # COLUMNS=`$CUR_DIR/00900_parquet_create_table_columns.py $JSON` 2>&1 || continue
    COLUMNS=$(cat "$COLUMNS_FILE") || continue

    ${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS parquet_load"
    $CLICKHOUSE_CLIENT --multiquery <<EOF
SET allow_experimental_map_type = 1;
CREATE TABLE parquet_load ($COLUMNS) ENGINE = Memory;
EOF

    # Some files contain unsupported data structures, exception is ok.
    cat "$DATA_DIR"/"$NAME" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO parquet_load FORMAT Parquet" 2>&1 | sed 's/Exception/Ex---tion/'

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM parquet_load LIMIT 100"
    ${CLICKHOUSE_CLIENT} --query="DROP TABLE parquet_load"
done
