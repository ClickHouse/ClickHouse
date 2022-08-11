#!/bin/bash

set -eo pipefail
shopt -s nullglob

DO_CHOWN=1
if [ "${CLICKHOUSE_DO_NOT_CHOWN:-0}" = "1" ]; then
    DO_CHOWN=0
fi

CLICKHOUSE_UID="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
CLICKHOUSE_GID="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"

# support --user
if [ "$(id -u)" = "0" ]; then
    USER=$CLICKHOUSE_UID
    GROUP=$CLICKHOUSE_GID
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    DO_CHOWN=0
fi

# set some vars
CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"

# get CH directories locations
DATA_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=path || true)"
TMP_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=tmp_path || true)"
USER_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=user_files_path || true)"
LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.log || true)"
LOG_DIR=""
if [ -n "$LOG_PATH" ]; then LOG_DIR="$(dirname "$LOG_PATH")"; fi
ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.errorlog || true)"
ERROR_LOG_DIR=""
if [ -n "$ERROR_LOG_PATH" ]; then ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH")"; fi
FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=format_schema_path || true)"

# There could be many disks declared in config
readarray -t FILESYSTEM_CACHE_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.data_cache_path' || true)
readarray -t DISKS_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.path' || true)

CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"
CLICKHOUSE_ACCESS_MANAGEMENT="${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-0}"

for dir in "$DATA_DIR" \
  "$ERROR_LOG_DIR" \
  "$LOG_DIR" \
  "$TMP_DIR" \
  "$USER_PATH" \
  "$FORMAT_SCHEMA_PATH" \
  "${FILESYSTEM_CACHE_PATHS[@]}" \
  "${DISKS_PATHS[@]}"
do
    # check if variable not empty
    [ -z "$dir" ] && continue
    # ensure directories exist
    if [ "$DO_CHOWN" = "1" ]; then
        mkdir="mkdir"
    else
        # if DO_CHOWN=0 it means that the system does not map root user to "admin" permissions
        # it mainly happens on NFS mounts where root==nobody for security reasons
        # thus mkdir MUST run with user id/gid and not from nobody that has zero permissions
        mkdir="/usr/bin/clickhouse su "${USER}:${GROUP}" mkdir"
    fi
    if ! $mkdir -p "$dir"; then
        echo "Couldn't create necessary directory: $dir"
        exit 1
    fi

    if [ "$DO_CHOWN" = "1" ]; then
        # ensure proper directories permissions
        # but skip it for if directory already has proper premissions, cause recursive chown may be slow
        if [ "$(stat -c %u "$dir")" != "$USER" ] || [ "$(stat -c %g "$dir")" != "$GROUP" ]; then
            chown -R "$USER:$GROUP" "$dir"
        fi
    fi
done

# if clickhouse user is defined - create it (user "default" already exists out of box)
if [ -n "$CLICKHOUSE_USER" ] && [ "$CLICKHOUSE_USER" != "default" ] || [ -n "$CLICKHOUSE_PASSWORD" ]; then
    echo "$0: create new user '$CLICKHOUSE_USER' instead 'default'"
    cat <<EOT > /etc/clickhouse-server/users.d/default-user.xml
    <clickhouse>
      <!-- Docs: <https://clickhouse.com/docs/en/operations/settings/settings_users/> -->
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
          <access_management>${CLICKHOUSE_ACCESS_MANAGEMENT}</access_management>
        </${CLICKHOUSE_USER}>
      </users>
    </clickhouse>
EOT
fi

if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
    # port is needed to check if clickhouse-server is ready for connections
    HTTP_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=http_port)"
    HTTPS_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=https_port)"
    
    if [ -n "$HTTP_PORT" ]; then
        URL="https://127.0.0.1:$HTTPS_PORT/ping"
    else
        URL="https://127.0.0.1:$HTTPS_PORT/ping"
    fi

    # Listen only on localhost until the initialization is done
    /usr/bin/clickhouse su "${USER}:${GROUP}" /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" -- --listen_host=127.0.0.1 &
    pid="$!"

    # check if clickhouse is ready to accept connections
    # will try to send ping clickhouse via http_port (max 12 retries by default, with 1 sec timeout and 1 sec delay between retries)
    tries=${CLICKHOUSE_INIT_TIMEOUT:-12}
    while ! wget --spider --no-check-certificate -T 1 -q "$URL" 2>/dev/null; do
        if [ "$tries" -le "0" ]; then
            echo >&2 'ClickHouse init process failed.'
            exit 1
        fi
        tries=$(( tries-1 ))
        sleep 1
    done

    clickhouseclient=( clickhouse-client --multiquery --host "127.0.0.1" -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" )

    echo

    # create default database, if defined
    if [ -n "$CLICKHOUSE_DB" ]; then
        echo "$0: create database '$CLICKHOUSE_DB'"
        "${clickhouseclient[@]}" -q "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB";
    fi

    for f in /docker-entrypoint-initdb.d/*; do
        case "$f" in
            *.sh)
                if [ -x "$f" ]; then
                    echo "$0: running $f"
                    "$f"
                else
                    echo "$0: sourcing $f"
                    # shellcheck source=/dev/null
                    . "$f"
                fi
                ;;
            *.sql)    echo "$0: running $f"; "${clickhouseclient[@]}" < "$f" ; echo ;;
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
    # Watchdog is launched by default, but does not send SIGINT to the main process,
    # so the container can't be finished by ctrl+c
    CLICKHOUSE_WATCHDOG_ENABLE=${CLICKHOUSE_WATCHDOG_ENABLE:-0}
    export CLICKHOUSE_WATCHDOG_ENABLE
    exec /usr/bin/clickhouse su "${USER}:${GROUP}" /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
