#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT "
    SELECT 'Policy for table \`*\` does not affect other tables in the database';
    CREATE ROW POLICY 02703_asterisk_${CLICKHOUSE_DATABASE}_policy ON ${CLICKHOUSE_DATABASE}.\`*\` USING x=1 AS permissive TO ALL;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.\`*\` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 100, 20;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.\`other\` (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x AS SELECT 100, 20;
    SELECT 'star', * FROM ${CLICKHOUSE_DATABASE}.\`*\`;
    SELECT 'other', * FROM ${CLICKHOUSE_DATABASE}.other;
    DROP ROW POLICY 02703_asterisk_${CLICKHOUSE_DATABASE}_policy ON ${CLICKHOUSE_DATABASE}.\`*\`;
"
