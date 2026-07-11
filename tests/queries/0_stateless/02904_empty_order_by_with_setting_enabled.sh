#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# setting enabled and no order by or primary key
${CLICKHOUSE_CLIENT} --query="
    SET create_table_empty_primary_key_by_default = true;
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8) ENGINE = MergeTree() SETTINGS index_granularity = 8192;
    SHOW CREATE TABLE test_empty_order_by;
" 2>&1 \ | grep -F -q "ORDER BY tuple()" && echo 'OK' || echo 'FAIL'

# setting enabled and per-column primary key
${CLICKHOUSE_CLIENT} --query="
    SET create_table_empty_primary_key_by_default = true;
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8 PRIMARY KEY, b String PRIMARY KEY) ENGINE = MergeTree() SETTINGS index_granularity = 8192;
    SHOW CREATE TABLE test_empty_order_by;
" 2>&1 \ | grep -F -q "ORDER BY (a, b)" && echo 'OK' || echo 'FAIL'

# setting enabled and primary key in table definition (not per-column or order by)
${CLICKHOUSE_CLIENT} --query="
    SET create_table_empty_primary_key_by_default = true;
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8, b String) ENGINE = MergeTree() PRIMARY KEY (a) SETTINGS index_granularity = 8192;
    SHOW CREATE TABLE test_empty_order_by;
" 2>&1 \ | grep -F -q "ORDER BY a" && echo 'OK' || echo 'FAIL'

# setting enabled and order by in table definition (no primary key)
${CLICKHOUSE_CLIENT} --query="
    SET create_table_empty_primary_key_by_default = true;
    DROP TABLE IF EXISTS test_empty_order_by;
    CREATE TABLE test_empty_order_by(a UInt8, b String) ENGINE = MergeTree() ORDER BY (a, b) SETTINGS index_granularity = 8192;
    SHOW CREATE TABLE test_empty_order_by;
" 2>&1 \ | grep -F -q "ORDER BY (a, b)"  && echo 'OK' || echo 'FAIL'
