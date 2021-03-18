#!/bin/bash

set -eo pipefail
shopt -s nullglob

# check to see if this file is being run or sourced from another script
_is_sourced() {
    # https://unix.stackexchange.com/a/215279
    [ "${#FUNCNAME[@]}" -ge 2 ] \
        && [ "${FUNCNAME[0]}" = '_is_sourced' ] \
        && [ "${FUNCNAME[1]}" = 'source' ]
}

get_variables() {
    # Backwards compatibility
    # Use logical/consistent name variable DO_NOT/DO
    CLICKHOUSE_DO_NOT_CHOWN="${CLICKHOUSE_DO_NOT_CHOWN:-0}"
    DO_CHOWN=1
    if [ "$CLICKHOUSE_DO_NOT_CHOWN" = "1" ]; then
        DO_CHOWN=0
    fi
    # Backwards compatibility
    INIT_ON_EVERY_START="${INIT_ON_EVERY_START:-1}"

    CLICKHOUSE_UID="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
    CLICKHOUSE_GID="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"

    CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"
    
    # port is needed to check if clickhouse-server is ready for connections
    HTTP_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=http_port)"

    # get CH directories locations
    DATA_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=path || true)"
    TMP_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=tmp_path || true)"
    USER_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=user_files_path || true)"
    LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.log || true)"
    LOG_DIR="$(dirname "$LOG_PATH" || true)"
    ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.errorlog || true)"
    ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH" || true)"
    FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=format_schema_path || true)"

    CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
    CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
    CLICKHOUSE_DB="${CLICKHOUSE_DB:-}"
    CLICKHOUSE_ACCESS_MANAGEMENT="${CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT:-0}"
    
    declare -g DATABASE_ALREADY_EXISTS
    if [ -d "$DATA_DIR/data" ]; then
        DATABASE_ALREADY_EXISTS='true'
    fi
}

setup_dirs() {
    echo "Setup directory permission:"
    local USER; USER="$(id -u)"
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
        if [ "$USER" = "0" ]; then
            # this will cause less disk access than `chown -R`
            find "$dir" \! -user clickhouse -exec chown clickhouse '{}' +
        fi
    done
}

create_clickhouse_user() {
    # if clickhouse user is defined - create it (user "default" already exists out of box)
    if [ -n "$CLICKHOUSE_USER" ] && [ "$CLICKHOUSE_USER" != "default" ] || [ -n "$CLICKHOUSE_PASSWORD" ]; then
        if [ ! -f "/etc/clickhouse-server/users.d/default-user.xml" ]; then
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
        <access_management>${CLICKHOUSE_ACCESS_MANAGEMENT}</access_management>
        </${CLICKHOUSE_USER}>
    </users>
    </yandex>
EOT
        else
            echo "User $CLICKHOUSE_USER already exist, skip user creation"
        fi
    fi
}

docker_process_init_files() {
    if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
        # Listen only on localhost until the initialization is done
        "$@" --config-file="$CLICKHOUSE_CONFIG" -- --listen_host=127.0.0.1 &
        pid="$!"

        # check if clickhouse is ready to accept connections
        # will try to send ping clickhouse via http_port (max 12 retries by default, with 1 sec timeout and 1 sec delay between retries)
        tries=${CLICKHOUSE_INIT_TIMEOUT:-12}
        while ! wget --spider -T 1 -q "http://127.0.0.1:$HTTP_PORT/ping" 2>/dev/null; do
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
}

_main() {
    if [ "${1:0:1}" = '-' ]; then
        set -- clickhouse-server "$@"
    fi
    if [ "$1" = 'clickhouse-server' ]; then
        get_variables
        create_clickhouse_user

        # By default always chown clickhouse directories
        # Use env. variable CLICKHOUSE_DO_NOT_CHOWN set to 1 to override
        if [ "$DO_CHOWN" = "1" ]; then
            setup_dirs
        else
            echo "Skipping directory setup"
        fi

        # If container is started as root user, restart as dedicated clickhouse user
        if [ "$(id -u)" = "0" ]; then
            echo "Switching to dedicated user 'clickhouse'"
            exec gosu clickhouse "$BASH_SOURCE" "$@"
        fi

        # Backwards compatibility:
        # Modify dir permission, create users and parse init_files
        # only INIT_ON_EVERY_START is equal to 1 (default)
        # 
        # To skip this step set INIT_ON_EVERY_START env. variable to 0
        if [ "$INIT_ON_EVERY_START" = "1" ]; then
            docker_process_init_files "$@"
        else
            echo "Skipping process init files"
        fi
        exec "$@" --config-file="$CLICKHOUSE_CONFIG"
    fi

    # Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
    exec "$@"
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
    _main "$@"
fi