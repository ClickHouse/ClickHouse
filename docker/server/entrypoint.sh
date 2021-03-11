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
    declare -g DO_CHOWN
    DO_CHOWN=1
    if [ "${CLICKHOUSE_DO_NOT_CHOWN:-0}" = "1" ]; then
        DO_CHOWN=0
    fi

    declare -g CLICKHOUSE_UID CLICKHOUSE_GID
    CLICKHOUSE_UID="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
    CLICKHOUSE_GID="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"

    declare -g gosu USER GROUP
    # support --user
    if [ "$(id -u)" = "0" ]; then
        USER=$CLICKHOUSE_UID
        GROUP=$CLICKHOUSE_GID
        if command -v gosu &> /dev/null; then
            gosu="gosu $USER:$GROUP"
        elif command -v su-exec &> /dev/null; then
            gosu="su-exec $USER:$GROUP"
        else
            echo "No gosu/su-exec detected!"
            exit 1
        fi
    else
        # User needs to exist in docker image
        # build a new image with the user desired
        USER="$(id -u)"
        GROUP="$(id -g)"
        gosu=""
        DO_CHOWN=0
    fi

    declare -g CLICKHOUSE_CONFIG SERVER_ARGS
    CLICKHOUSE_CONFIG="${CLICKHOUSE_CONFIG:-/etc/clickhouse-server/config.xml}"
    SERVER_ARGS="$SERVER_ARGS --config-file=$CLICKHOUSE_CONFIG"

    if ! $gosu test -f "$CLICKHOUSE_CONFIG" -a -r "$CLICKHOUSE_CONFIG"; then
        echo "Configuration file '$dir' isn't readable by user with id '$USER'"
        exit 1
    fi

    declare -g HTTP_PORT
    # port is needed to check if clickhouse-server is ready for connections
    HTTP_PORT="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=http_port)"

    declare -g DATA_DIR TMP_DIR USER_PATH LOG_PATH LOG_DIR ERROR_LOG_PATH ERROR_LOG_DIR FORMAT_SCHEMA_PATH
    # get CH directories locations
    DATA_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=path || true)"
    TMP_DIR="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=tmp_path || true)"
    USER_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=user_files_path || true)"
    LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.log || true)"
    LOG_DIR="$(dirname "$LOG_PATH" || true)"
    ERROR_LOG_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=logger.errorlog || true)"
    ERROR_LOG_DIR="$(dirname "$ERROR_LOG_PATH" || true)"
    FORMAT_SCHEMA_PATH="$(clickhouse extract-from-config --config-file "$CLICKHOUSE_CONFIG" --key=format_schema_path || true)"

    declare -g CLICKHOUSE_USER CLICKHOUSE_PASSWORD CLICKHOUSE_DB CLICKHOUSE_ACCESS_MANAGEMENT
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
        elif ! $gosu test -d "$dir" -a -w "$dir" -a -r "$dir"; then
            echo "Necessary directory '$dir' isn't accessible by user with id '$USER'"
            exit 1
        fi
    done
}

create_clickhouse_user() {
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
        <access_management>${CLICKHOUSE_ACCESS_MANAGEMENT}</access_management>
        </${CLICKHOUSE_USER}>
    </users>
    </yandex>
EOT
    fi
}

docker_process_init_files() {
    if [ -n "$(ls /docker-entrypoint-initdb.d/)" ] || [ -n "$CLICKHOUSE_DB" ]; then
        # Listen only on localhost until the initialization is done
        $gosu /usr/bin/clickhouse-server --config-file="$CLICKHOUSE_CONFIG" -- --listen_host=127.0.0.1 &
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
        echo $f
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
        # Modify dir permission, create users and parse init_files
        # only if data directory is empyt
        if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
            setup_dirs
            create_clickhouse_user
            docker_process_init_files
        fi
    fi

    # SERVER_ARGS and gosu variables are defined in get_variables 
    # and set_user functions.
    # These variables are defined only if the first argument is clickhouse-server
    exec $gosu "$@" $SERVER_ARGS
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
	_main "$@"
fi
