#!/bin/bash

set -eo pipefail
shopt -s nullglob

DO_CHOWN=1
if [[ "${CLICKHOUSE_RUN_AS_ROOT:=0}" = "1" || "${CLICKHOUSE_DO_NOT_CHOWN:-0}" = "1" ]]; then
    DO_CHOWN=0
fi

# CLICKHOUSE_UID and CLICKHOUSE_GID are kept for backward compatibility, but deprecated
# One must use either "docker run --user" or CLICKHOUSE_RUN_AS_ROOT=1 to run the process as
# FIXME: Remove ALL CLICKHOUSE_UID CLICKHOUSE_GID before 25.3
if [[ "${CLICKHOUSE_UID:-}" || "${CLICKHOUSE_GID:-}" ]]; then
    echo 'WARNING: Support for CLICKHOUSE_UID/CLICKHOUSE_GID will be removed in a couple of releases.' >&2
    echo 'WARNING: Either use a proper "docker run --user=xxx:xxxx" argument instead of CLICKHOUSE_UID/CLICKHOUSE_GID' >&2
    echo 'WARNING: or set "CLICKHOUSE_RUN_AS_ROOT=1" ENV to run the clickhouse-server as root:root' >&2
fi

# support `docker run --user=xxx:xxxx`
if [[ "$(id -u)" = "0" ]]; then
    if [[ "$CLICKHOUSE_RUN_AS_ROOT" = 1 ]]; then
        USER=0
        GROUP=0
    else
        USER="${CLICKHOUSE_UID:-"$(id -u clickhouse)"}"
        GROUP="${CLICKHOUSE_GID:-"$(id -g clickhouse)"}"
    fi
    if command -v gosu &> /dev/null; then
        gosu="gosu $USER:$GROUP"
    elif command -v su-exec &> /dev/null; then
        gosu="su-exec $USER:$GROUP"
    else
        echo "No gosu/su-exec detected!"
        exit 1
    fi
else
    USER="$(id -u)"
    GROUP="$(id -g)"
    gosu=""
    DO_CHOWN=0
fi

KEEPER_CONFIG="${KEEPER_CONFIG:-/etc/clickhouse-keeper/keeper_config.xml}"

if [ -f "$KEEPER_CONFIG" ] && ! $gosu test -f "$KEEPER_CONFIG" -a -r "$KEEPER_CONFIG"; then
    echo "Configuration file '$KEEPER_CONFIG' isn't readable by user with id '$USER'"
    exit 1
fi

DATA_DIR="${CLICKHOUSE_DATA_DIR:-/var/lib/clickhouse}"
LOG_DIR="${LOG_DIR:-/var/log/clickhouse-keeper}"
LOG_PATH="${LOG_DIR}/clickhouse-keeper.log"
ERROR_LOG_PATH="${LOG_DIR}/clickhouse-keeper.err.log"
COORDINATION_DIR="${DATA_DIR}/coordination"
COORDINATION_LOG_DIR="${DATA_DIR}/coordination/log"
COORDINATION_SNAPSHOT_DIR="${DATA_DIR}/coordination/snapshots"
CLICKHOUSE_WATCHDOG_ENABLE=${CLICKHOUSE_WATCHDOG_ENABLE:-0}

for dir in "$DATA_DIR" \
  "$LOG_DIR" \
  "$TMP_DIR" \
  "$COORDINATION_DIR" \
  "$COORDINATION_LOG_DIR" \
  "$COORDINATION_SNAPSHOT_DIR"
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
        # but skip it for if directory already has proper premissions, cause recursive chown may be slow
        if [ "$(stat -c %u "$dir")" != "$USER" ] || [ "$(stat -c %g "$dir")" != "$GROUP" ]; then
            chown -R "$USER:$GROUP" "$dir"
        fi
    elif ! $gosu test -d "$dir" -a -w "$dir" -a -r "$dir"; then
        echo "Necessary directory '$dir' isn't accessible by user with id '$USER'"
        exit 1
    fi
done

# if no args passed to `docker run` or first argument start with `--`, then the user is passing clickhouse-server arguments
if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
    # Watchdog is launched by default, but does not send SIGINT to the main process,
    # so the container can't be finished by ctrl+c
    export CLICKHOUSE_WATCHDOG_ENABLE

    cd "${DATA_DIR}"

    # There is a config file. It is already tested with gosu (if it is readably by keeper user)
    if [ -f "$KEEPER_CONFIG" ]; then
        exec $gosu clickhouse-keeper --config-file="$KEEPER_CONFIG" --log-file="$LOG_PATH" --errorlog-file="$ERROR_LOG_PATH" "$@"
    fi

    # There is no config file. Will use embedded one
    exec $gosu clickhouse-keeper --log-file="$LOG_PATH" --errorlog-file="$ERROR_LOG_PATH" "$@"
fi

# Otherwise, we assume the user want to run his own process, for example a `bash` shell to explore this image
exec "$@"
