#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_too_many_parts_01902"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_too_many_parts_01902 (A Int64, D date) \
                       ENGINE = MergeTree partition by D order by A \
                       SETTINGS parts_to_throw_insert = 10"

$CLICKHOUSE_CLIENT -q "system stop merges test_too_many_parts_01902"

$CLICKHOUSE_CLIENT -q "insert into test_too_many_parts_01902 \
                       select *, today() from numbers(10) \
                       settings max_block_size=1, min_insert_block_size_rows=1, \
                       min_insert_block_size_bytes=1"

$CLICKHOUSE_CLIENT -q "select 'step 1'"

$CLICKHOUSE_CLIENT -q "insert into test_too_many_parts_01902 \
                       select *, today() from numbers(10) \
                       settings max_block_size=1, min_insert_block_size_rows=1, \
                       min_insert_block_size_bytes=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_too_many_parts_01902" 2>&1 | \
    sed 's/DB::Exception: Received from .*\. DB::Exception: Too many parts (.*)\./DB::Exception: Too many parts./g'

