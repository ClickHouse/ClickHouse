#!/bin/bash

# set some vars
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"
USER="$(id -u clickhouse)"
GROUP="$(id -g clickhouse)"

# get CH directories locations
DATA_DIR="$(grep -oP '<path>\K(.*)(?=[/?]</path>)' $CLICKHOUSE_CONFIG || true)"
TMP_DIR="$(grep -oP '<tmp_path>\K(.*)(?=[/?]</tmp_path>)' $CLICKHOUSE_CONFIG || true)"
USER_PATH="$(grep -oP '<user_files_path>\K(.*)(?=</user_files_path>)' $CLICKHOUSE_CONFIG || true)"
LOG_PATH="$(grep -oP '<log>\K(.*)(?=</log>)' $CLICKHOUSE_CONFIG || true)"
LOG_DIR="$(dirname $LOG_PATH || true)"
ERROR_LOG_PATH="$(grep -oP '<errorlog>\K(.*)(?=</errorlog>)' $CLICKHOUSE_CONFIG || true)"
ERROR_LOG_DIR="$(dirname $ERROR_LOG_PATH || true)"

# ensure directories exist
mkdir -p \
    "$DATA_DIR" \
    "$ERROR_LOG_DIR" \
    "$LOG_DIR" \
    "$TMP_DIR" \
    "$USER_PATH"

# ensure proper directories permissions
chown -R $USER:$GROUP \
    "$DATA_DIR" \
    "$ERROR_LOG_DIR" \
    "$LOG_DIR" \
    "$TMP_DIR" \
    "$USER_PATH"

# execute CMD
exec gosu clickhouse "$@"
