#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists test_02243"
$CLICKHOUSE_CLIENT -q "create table test_02243 (image_path Nullable(String),
                                caption Nullable(String),
                                NSFW Nullable(String),
                                similarity Nullable(Float64),
                                LICENSE Nullable(String),
                                url Nullable(String),
                                key Nullable(UInt64),
                                shard_id Nullable(UInt64),
                                status Nullable(String),
                                error_message Nullable(String),
                                width Nullable(UInt32),
                                height Nullable(UInt32),
                                exif Nullable(String),
                                original_width Nullable(UInt32),
                                original_height Nullable(UInt32)) engine=Memory"

cat $CUR_DIR/data_parquet_bad_column/metadata_0.parquet | $CLICKHOUSE_CLIENT  --stacktrace -q "insert into test_02243 format Parquet"

$CLICKHOUSE_CLIENT -q "select count() from test_02243"
$CLICKHOUSE_CLIENT -q "drop table test_02243"
