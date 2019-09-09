#!/bin/bash

# set some vars
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"
if [ x"$UID" == x0 ]; then
    USER="$(id -u clickhouse)"
    GROUP="$(id -g clickhouse)"
    gosu="gosu $USER:$GROUP"
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    gosu=""
fi

# port is needed to check if clickhouse-server is ready for connections
HTTP_PORT="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=http_port)"

# get CH directories locations
DATA_DIR="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=path || true)"
TMP_DIR="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=tmp_path || true)"
USER_PATH="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=user_files_path || true)"
LOG_PATH="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=logger.log || true)"
LOG_DIR="$(dirname $LOG_PATH || true)"
ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=logger.errorlog || true)"
ERROR_LOG_DIR="$(dirname $ERROR_LOG_PATH || true)"
FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file $CLICKHOUSE_CONFIG --key=format_schema_path || true)"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"

for dir in "$DATA_DIR" \
  "$ERROR_LOG_DIR" \
  "$LOG_DIR" \
  "$TMP_DIR" \
  "$USER_PATH" \
  "$FORMAT_SCHEMA_PATH"
do
    # check if variable not empty
    [ -z "$dir" ] && continue
    # ensure directories exist
    if ! mkdir -p "$dir"; then
        echo "Couldn't create necessary directory: $dir"
        exit 1
    fi

    if [ x"$UID" == x0 ] && [ "$CLICKHOUSE_DO_NOT_CHOWN" != "1" ]; then
        # ensure proper directories permissions
        chown -R "$USER:$GROUP" "$dir"
    elif [ "$(stat -c %u "$dir")" != "$USER" ]; then
        echo "Necessary directory '$dir' isn't owned by user with id '$USER'"
        exit 1
    fi
done



if [ -n "$(ls /docker-entrypoint-initdb.d/)" ]; then
    $gosu /usr/bin/clickhouse-server --config-file=$CLICKHOUSE_CONFIG &
    pid="$!"

    # check if clickhouse is ready to accept connections
    # will try to send ping clickhouse via http_port (max 12 retries, with 1 sec delay)
    if ! wget --spider --quiet --tries=12 --waitretry=1 --retry-connrefused "http://localhost:$HTTP_PORT/ping" ; then
        echo >&2 'ClickHouse init process failed.'
        exit 1
    fi

    if [ ! -z "$CLICKHOUSE_PASSWORD" ]; then
        printf -v WITH_PASSWORD '%s %q' "--password" "$CLICKHOUSE_PASSWORD"
    fi

    clickhouseclient=( clickhouse-client --multiquery -u $CLICKHOUSE_USER $WITH_PASSWORD )

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
        echo >&2 'Finishing of ClickHouse init process failed.'
        exit 1
    fi
fi

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    exec $gosu /usr/bin/clickhouse-server --config-file=$CLICKHOUSE_CONFIG "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
