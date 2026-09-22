#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="$CLICKHOUSE_TMP/05238_dictionary_settings_unknown_name_stored"
rm -rf "${WORKING_DIR:?}"
mkdir -p "$WORKING_DIR"

$CLICKHOUSE_LOCAL --path "$WORKING_DIR" -m -q "
CREATE TABLE src (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, 42);
CREATE DICTIONARY dict (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB 'default')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(max_result_bytes = 1000000);
SELECT 'stored', dictGet('dict', 'v', toUInt64(1));
" < /dev/null

METADATA=$(find "$WORKING_DIR" -name 'dict.sql' | head -1)
sed -i 's/max_result_bytes/not_a_setting_at_all/' "$METADATA"
# Without this the following arms would pass on an unedited definition and assert nothing.
echo -n 'edited '
grep -c -m 1 -F 'not_a_setting_at_all' "$METADATA"

$CLICKHOUSE_LOCAL --path "$WORKING_DIR" -m -q "
SELECT 'loads', dictGet('dict', 'v', toUInt64(1));
SELECT 'keeps name', countSubstrings(create_table_query, 'not_a_setting_at_all') FROM system.tables WHERE database = currentDatabase() AND name = 'dict';
" < /dev/null

$CLICKHOUSE_LOCAL --path "$WORKING_DIR" -m -q "
DETACH DICTIONARY dict;
ATTACH DICTIONARY dict;
SELECT 'reattaches', dictGet('dict', 'v', toUInt64(1));
SELECT 'keeps name', countSubstrings(create_table_query, 'not_a_setting_at_all') FROM system.tables WHERE database = currentDatabase() AND name = 'dict';
" < /dev/null

$CLICKHOUSE_LOCAL --path "$WORKING_DIR" -m -q "
CREATE DICTIONARY restated (id UInt64, v UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'src' DB 'default')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 0)
SETTINGS(not_a_setting_at_all = 1000000);
" < /dev/null 2>&1 | grep -o -m 1 'UNKNOWN_SETTING'

rm -rf "${WORKING_DIR:?}"
