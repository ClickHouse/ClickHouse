#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SESSION_ID_A="$RANDOM$RANDOM$RANDOM"
SESSION_ID_B="$RANDOM$RANDOM$RANDOM"

# Create temporary table and insert in SESSION_ID_A
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_A}" -d 'CREATE TEMPORARY TABLE table_merge_tree_02561 (id UInt64, info String) ENGINE = MergeTree ORDER BY id'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_A}" -d "INSERT INTO table_merge_tree_02561 VALUES (1, 'a'), (2, 'b'), (3, 'c')"

# Select from SESSION_ID_B
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_B}" -d "SELECT * FROM table_merge_tree_02561" | tr -d '\n' | grep -F 'UNKNOWN_TABLE' > /dev/null && echo "OK"

# Create temporary table, insert and select in SESSION_ID_B
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_B}" -d 'CREATE TEMPORARY TABLE table_merge_tree_02561 (id UInt64, info String) ENGINE = MergeTree ORDER BY id'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_B}" -d "INSERT INTO table_merge_tree_02561 VALUES (1, 'd'), (2, 'e'), (3, 'f')"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_B}" -d "SELECT * FROM table_merge_tree_02561"

# Select from SESSION_ID_A
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_A}" -d "SELECT * FROM table_merge_tree_02561"

# Drop tables in both sessions
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_A}" -d "DROP TEMPORARY TABLE table_merge_tree_02561"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${SESSION_ID_B}" -d "DROP TEMPORARY TABLE table_merge_tree_02561"
