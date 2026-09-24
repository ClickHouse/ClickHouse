#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="insert into function file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv') select tuple(0, 'Hello') settings engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT --query="drop table if exists test;"
$CLICKHOUSE_CLIENT --query="create table test (a Tuple(Int32, Int32)) engine = MergeTree order by tuple() settings min_bytes_for_wide_part=1, ratio_of_defaults_for_sparse_serialization=0.01;"
$CLICKHOUSE_CLIENT --query="insert into test select tuple(0, 0) from numbers(100)"
$CLICKHOUSE_CLIENT --query="insert into test select * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.csv') settings input_format_allow_errors_num=1"
$CLICKHOUSE_CLIENT --query="drop table test;"