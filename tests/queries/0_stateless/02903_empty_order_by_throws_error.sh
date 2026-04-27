#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# setting disabled and no order by or primary key; expect error
$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8) ENGINE = MergeTree() SETTINGS index_granularity = 8192;
" 2>&1 \ | grep -F -q "You must provide an ORDER BY or PRIMARY KEY expression in the table definition." && echo 'OK' || echo 'FAIL'

# setting disabled and primary key in table definition
$CLICKHOUSE_CLIENT --query="
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8) ENGINE = MergeTree() PRIMARY KEY a SETTINGS index_granularity = 8192;
    SHOW CREATE TABLE test_empty_order_by;
" 2>&1 \ | grep -F -q "ORDER BY a" && echo 'OK' || echo 'FAIL'
