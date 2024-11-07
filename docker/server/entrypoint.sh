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
readarray -t DISKS_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.path' || true)
readarray -t DISKS_METADATA_PATHS < <(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key='storage_configuration.disks.*.metadata_path' || true)

CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-}"
if [[ -n "${CLICKHOUSE_PASSWORD_FILE}" && -f "${CLICKHOUSE_PASSWORD_FILE}" ]]; then
  CLICKHOUSE_PASSWORD="$(cat "${CLICKHOUSE_PASSWORD_FILE}")"
fi
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"
CLICKHOUSE_ACCESS_MANAGEMENT="${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-0}"

function create_directory_and_do_chown() {
    local dir=$1
    # check if variable not empty
    [ -z "$dir" ] && return
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
}

create_directory_and_do_chown "$DATA_DIR"

# Change working directory to $DATA_DIR in case there're paths relative to $DATA_DIR, also avoids running
# clickhouse-server at root directory.
cd "$DATA_DIR"

for dir in "$ERROR_LOG_DIR" \
  "$LOG_DIR" \
  "$TMP_DIR" \
  "$USER_PATH" \
  "$FORMAT_SCHEMA_PATH" \
  "${DISKS_PATHS[@]}" \
  "${DISKS_METADATA_PATHS[@]}"
do
    create_directory_and_do_chown "$dir"
done

# if clickhouse user is defined - create it (user "default" already exists out of box)
if [ -n "$CLICKHOUSE_USER" ] && [ "$CLICKHOUSE_USER" != "default" ] || [ -n "$CLICKHOUSE_PASSWORD" ] || [ "$CLICKHOUSE_ACCESS_MANAGEMENT" != "0" ]; then
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
          <password><![CDATA[${CLICKHOUSE_PASSWORD//]]>/]]]]><![CDATA[>}]]></password>
          <quota>default</quota>
          <access_management>${CLICKHOUSE_ACCESS_MANAGEMENT}</access_management>
        </${CLICKHOUSE_USER}>
      </users>
    </clickhouse>
EOT
fi

CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS="${CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS:-}"

# checking $DATA_DIR for initialization
if [ -d "${DATA_DIR%/}/data" ]; then
    DATABASE_ALREADY_EXISTS='true'
fi

# run initialization if flag CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS is not empty or data directory is empty
if [[ -n "${CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS}" || -z "${DATABASE_ALREADY_EXISTS}" ]]; then
  RUN_INITDB_SCRIPTS='true'
fi

if [ -n "${RUN_INITDB_SCRIPTS}" ]; then
    if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
        # port is needed to check if clickhouse-server is ready for connections
        HTTP_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=http_port --try)"
        HTTPS_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=https_port --try)"

        if [ -n "$HTTP_PORT" ]; then
            URL="http://127.0.0.1:$HTTP_PORT/ping"
        else
            URL="https://127.0.0.1:$HTTPS_PORT/ping"
        fi

        # Listen only on localhost until the initialization is done
        /usr/bin/clickhouse su "${USER}:${GROUP}" /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" -- --listen_host=127.0.0.1 &
        pid="$!"

        # check if clickhouse is ready to accept connections
        # will try to send ping clickhouse via http_port (max 1000 retries by default, with 1 sec timeout and 1 sec delay between retries)
        tries=${CLICKHOUSE_INIT_TIMEOUT:-1000}
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
else
    echo "ClickHouse Database directory appears to contain a database; Skipping initialization"
fi

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    # Watchdog is launched by default, but does not send SIGINT to the main process,
    # so the container can't be finished by ctrl+c
    CLICKHOUSE_WATCHDOG_ENABLE=${CLICKHOUSE_WATCHDOG_ENABLE:-0}
    export CLICKHOUSE_WATCHDOG_ENABLE

    # An option for easy restarting and replacing clickhouse-server in a container, especially in Kubernetes.
    # For example, you can replace the clickhouse-server binary to another and restart it while keeping the container running.
    if [[ "${CLICKHOUSE_DOCKER_RESTART_ON_EXIT:-0}" -eq "1" ]]; then
        while true; do
            # This runs the server as a child process of the shell script:
            /usr/bin/clickhouse su "${USER}:${GROUP}" /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "$@" ||:
            echo >&2 'ClickHouse Server exited, and the environment variable CLICKHOUSE_DOCKER_RESTART_ON_EXIT is set to 1. Restarting the server.'
        done
    else
        # This replaces the shell script with the server:
        exec /usr/bin/clickhouse su "${USER}:${GROUP}" /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" "$@"
    fi
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
