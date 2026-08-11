#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE key_column (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
    ALTER TABLE key_column DROP COLUMN a;
" 2>&1 | grep -oF 'Trying to ALTER DROP key a column which is a part of key expression'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE subcolumn (a Tuple(x UInt64, y UInt64), b UInt64) ENGINE = MergeTree ORDER BY a.x;
    ALTER TABLE subcolumn DROP COLUMN a;
" 2>&1 | grep -oF 'Trying to ALTER DROP column a whose subcolumns (`a.x`) are part of key expression'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE sign_column (a UInt64, s Int8) ENGINE = CollapsingMergeTree(s) ORDER BY a;
    ALTER TABLE sign_column DROP COLUMN s;
" 2>&1 | grep -oF 'Trying to ALTER DROP sign (s) column'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE table_ttl (d Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY;
    ALTER TABLE table_ttl DROP COLUMN d;
" 2>&1 | grep -oF 'Cannot apply ALTER because it breaks the TTL of the table'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE column_ttl (d Date, a UInt64, v UInt64 TTL d + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY a;
    ALTER TABLE column_ttl DROP COLUMN d;
" 2>&1 | grep -oF 'Cannot apply ALTER because it breaks the TTL of column `v`'
