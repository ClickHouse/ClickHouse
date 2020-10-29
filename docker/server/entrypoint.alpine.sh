#!/bin/sh
#set -x

DO_CHOWN=1
if [ "$CLICKHOUSE_DO_NOT_CHOWN" = 1 ]; then
    DO_CHOWN=0
fi

CLICKHOUSE_UID="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
CLICKHOUSE_GID="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"

# support --user
if [ "$(id -u)" = "0" ]; then
    USER=$CLICKHOUSE_UID
    GROUP=$CLICKHOUSE_GID
    # busybox has setuidgid & chpst buildin
    gosu="su-exec $USER:$GROUP"
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    gosu=""
    DO_CHOWN=0
fi

# set some vars
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

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
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"

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

    if [ "$DO_CHOWN" = "1" ]; then
        # ensure proper directories permissions
        chown -R "$USER:$GROUP" "$dir"
    elif [ "$(stat -c %u "$dir")" != "$USER" ]; then
        echo "Necessary directory '$dir' isn't owned by user with id '$USER'"
        exit 1
    fi
done

# if clickhouse user is defined - create it (user "default" already exists out of box)
if [ -n "$CLICKHOUSE_USER" ] && [ "$CLICKHOUSE_USER" != "default" ] || [ -n "$CLICKHOUSE_PASSWORD" ]; then
    echo "$0: create new user '$CLICKHOUSE_USER' instead 'default'"
    cat <<EOT > /etc/clickhouse-server/users.d/default-user.xml
    <yandex>
      <!-- Docs: <https://clickhouse.tech/docs/en/operations/settings/settings_users/> -->
      <users>
        <!-- Remove default user -->
        <default remove="remove">
        </default>

        <${CLICKHOUSE_USER}>
          <profile>default</profile>
          <networks>
            <ip>::/0</ip>
          </networks>
          <password>${CLICKHOUSE_PASSWORD}</password>
          <quota>default</quota>
        </${CLICKHOUSE_USER}>
      </users>
    </yandex>
EOT
fi

if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
    # Listen only on localhost until the initialization is done
    $gosu /usr/bin/clickhouse-server --config-file=$CLICKHOUSE_CONFIG -- --listen_host=127.0.0.1 &
    pid="$!"

    # check if clickhouse is ready to accept connections
    # will try to send ping clickhouse via http_port (max 6 retries, with 1 sec timeout and 1 sec delay between retries)
    tries=6
    while ! wget --spider -T 1 -q "http://localhost:$HTTP_PORT/ping" 2>/dev/null; do
        if [ "$tries" -le "0" ]; then
            echo >&2 'ClickHouse init process failed.'
            exit 1
        fi
        tries=$(( tries-1 ))
        sleep 1
    done

    if [ ! -z "$CLICKHOUSE_PASSWORD" ]; then
        printf -v WITH_PASSWORD '%s %q' "--password" "$CLICKHOUSE_PASSWORD"
    fi

    clickhouseclient="clickhouse-client --multiquery -u $CLICKHOUSE_USER $WITH_PASSWORD "

    # create default database, if defined
    if [ -n "$CLICKHOUSE_DB" ]; then
        echo "$0: create database '$CLICKHOUSE_DB'"
        "$clickhouseclient" -q "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB";
    fi

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
            *.sql)    echo "$0: running $f"; cat "$f" | "$clickhouseclient" ; echo ;;
            *.sql.gz) echo "$0: running $f"; gunzip -c "$f" | "$clickhouseclient"; echo ;;
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
