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

if [ -n "$(ls /docker-entrypoint-initdb.d/)" ]; then
    gosu clickhouse /usr/bin/clickhouse-server --config-file=$CLICKHOUSE_CONFIG &
    pid="$!"
    sleep 1

    clickhouseclient=( clickhouse client )
    echo
    for f in /docker-entrypoint-initdb.d/*; do
        case "$f" in
            *.sh)
                if [ -x "$f" ]; then
                    echo "$0: running $f"
                    "$f"
                else
                    echo "$0: sourcing $f"
                    . "$f"
                fi
                ;;
            *.sql)    echo "$0: running $f"; cat "$f" | "${clickhouseclient[@]}" ; echo ;;
            *.sql.gz) echo "$0: running $f"; gunzip -c "$f" | "${clickhouseclient[@]}"; echo ;;
            *)        echo "$0: ignoring $f" ;;
        esac
        echo
    done

    if ! kill -s TERM "$pid" || ! wait "$pid"; then
        echo >&2 'ClickHouse init process failed.'
        exit 1
    fi
fi

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    exec gosu clickhouse /usr/bin/clickhouse-server --config-file=$CLICKHOUSE_CONFIG "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
