#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for case_insensitive in "true" "false"; do
    $CLICKHOUSE_CLIENT -q "drop table if exists test_02241"
    $CLICKHOUSE_CLIENT -q "create table test_02241 (image_path Nullable(String),
                                    caption Nullable(String),
                                    NSFW Nullable(String),
                                    similarity Nullable(Float64),
                                    LICENSE Nullable(String),
                                    url Nullable(String),
                                    key Nullable(UInt64),
                                    shard_id Nullable(UInt64),
                                    status Nullable(String),
                                    width Nullable(UInt32),
                                    height Nullable(UInt32),
                                    exif Nullable(String),
                                    original_width Nullable(UInt32),
                                    original_height Nullable(UInt32)) engine=Memory"

    cat $CUR_DIR/data_parquet_bad_column/metadata_0.parquet | $CLICKHOUSE_CLIENT -q "insert into test_02241 SETTINGS input_format_parquet_case_insensitive_column_matching=$case_insensitive format Parquet"

    $CLICKHOUSE_CLIENT -q "select count() from test_02241"
    $CLICKHOUSE_CLIENT -q "drop table test_02241"
done
