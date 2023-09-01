#!/bin/bash

# This script sets up export of system log tables to a remote server.
# Remote tables are created if not exist, and augmented with extra columns,
# and their names will contain a hash of the table structure,
# which allows exporting tables from servers of different versions.

# Config file contains KEY=VALUE pairs with any necessary parameters like:
# CLICKHOUSE_CI_LOGS_HOST - remote host
# CLICKHOUSE_CI_LOGS_USER - password for user
# CLICKHOUSE_CI_LOGS_PASSWORD - password for user
CLICKHOUSE_CI_LOGS_CREDENTIALS=${CLICKHOUSE_CI_LOGS_CREDENTIALS:-/tmp/export-logs-config.sh}
CLICKHOUSE_CI_LOGS_USER=${CLICKHOUSE_CI_LOGS_USER:-ci}

# Pre-configured destination cluster, where to export the data
CLICKHOUSE_CI_LOGS_CLUSTER=${CLICKHOUSE_CI_LOGS_CLUSTER:-system_logs_export}

EXTRA_COLUMNS=${EXTRA_COLUMNS:-"pull_request_number UInt32, commit_sha String, check_start_time DateTime, check_name LowCardinality(String), instance_type LowCardinality(String), "}
EXTRA_COLUMNS_EXPRESSION=${EXTRA_COLUMNS_EXPRESSION:-"0 AS pull_request_number, '' AS commit_sha, now() AS check_start_time, '' AS check_name, '' AS instance_type"}
EXTRA_ORDER_BY_COLUMNS=${EXTRA_ORDER_BY_COLUMNS:-"check_name, "}

function __set_connection_args
{
    # It's impossible to use generous $CONNECTION_ARGS string, it's unsafe from word splitting perspective.
    # That's why we must stick to the generated option
    CONNECTION_ARGS=(
        --receive_timeout=45 --send_timeout=45 --secure
        --user "${CLICKHOUSE_CI_LOGS_USER}" --host "${CLICKHOUSE_CI_LOGS_HOST}"
        --password "${CLICKHOUSE_CI_LOGS_PASSWORD}"
    )
}

function __shadow_credentials
{
    # The function completely screws the output, it shouldn't be used in normal functions, only in ()
    # The only way to substitute the env as a plain text is using perl 's/\Qsomething\E/another/
    exec &> >(perl -pe '
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_HOST}\E)[CLICKHOUSE_CI_LOGS_HOST]g;
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_USER}\E)[CLICKHOUSE_CI_LOGS_USER]g;
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_PASSWORD}\E)[CLICKHOUSE_CI_LOGS_PASSWORD]g;
    ')
}

function check_logs_credentials
(
    # The function connects with given credentials, and if it's unable to execute the simplest query, returns exit code

    # First check, if all necessary parameters are set
    set +x
    for parameter in CLICKHOUSE_CI_LOGS_HOST CLICKHOUSE_CI_LOGS_USER CLICKHOUSE_CI_LOGS_PASSWORD; do
      export -p | grep -q "$parameter" || {
        echo "Credentials parameter $parameter is unset"
        return 1
      }
    done

    __shadow_credentials
    __set_connection_args
    local code
    # Catch both success and error to not fail on `set -e`
    clickhouse-client "${CONNECTION_ARGS[@]}" -q 'SELECT 1 FORMAT Null' && return 0 || code=$?
    if [ "$code" != 0 ]; then
        echo 'Failed to connect to CI Logs cluster'
        return $code
    fi
)

function config_logs_export_cluster
(
    # The function is launched in a separate shell instance to not expose the
    # exported values from CLICKHOUSE_CI_LOGS_CREDENTIALS
    set +x
    if ! [ -r "${CLICKHOUSE_CI_LOGS_CREDENTIALS}" ]; then
        echo "File $CLICKHOUSE_CI_LOGS_CREDENTIALS does not exist, do not setup"
        return
    fi
    set -a
    # shellcheck disable=SC1090
    source "${CLICKHOUSE_CI_LOGS_CREDENTIALS}"
    set +a
    __shadow_credentials
    echo "Checking if the credentials work"
    check_logs_credentials || return 0
    cluster_config="${1:-/etc/clickhouse-server/config.d/system_logs_export.yaml}"
    mkdir -p "$(dirname "$cluster_config")"
    echo "remote_servers:
    ${CLICKHOUSE_CI_LOGS_CLUSTER}:
        shard:
            replica:
                secure: 1
                user: '${CLICKHOUSE_CI_LOGS_USER}'
                host: '${CLICKHOUSE_CI_LOGS_HOST}'
                port: 9440
                password: '${CLICKHOUSE_CI_LOGS_PASSWORD}'
" > "$cluster_config"
    echo "Cluster ${CLICKHOUSE_CI_LOGS_CLUSTER} is confugured in ${cluster_config}"
)

function setup_logs_replication
(
    # The function is launched in a separate shell instance to not expose the
    # exported values from CLICKHOUSE_CI_LOGS_CREDENTIALS
    set +x
    # disable output
    if ! [ -r "${CLICKHOUSE_CI_LOGS_CREDENTIALS}" ]; then
        echo "File $CLICKHOUSE_CI_LOGS_CREDENTIALS does not exist, do not setup"
        return 0
    fi
    set -a
    # shellcheck disable=SC1090
    source "${CLICKHOUSE_CI_LOGS_CREDENTIALS}"
    set +a
    __shadow_credentials
    echo "Checking if the credentials work"
    check_logs_credentials || return 0
    __set_connection_args

    echo 'Create all configured system logs'
    clickhouse-client --query "SYSTEM FLUSH LOGS"

    # It's doesn't make sense to try creating tables if SYNC fails
    echo "SYSTEM SYNC DATABASE REPLICA default" | clickhouse-client "${CONNECTION_ARGS[@]}" || return 0

    # For each system log table:
    echo 'Create %_log tables'
    clickhouse-client --query "SHOW TABLES FROM system LIKE '%\\_log'" | while read -r table
    do
        # Calculate hash of its structure:
        hash=$(clickhouse-client --query "
            SELECT sipHash64(groupArray((name, type)))
            FROM (SELECT name, type FROM system.columns
                WHERE database = 'system' AND table = '$table'
                ORDER BY position)
            ")

        # Create the destination table with adapted name and structure:
        statement=$(clickhouse-client --format TSVRaw --query "SHOW CREATE TABLE system.${table}" | sed -r -e '
            s/^\($/('"$EXTRA_COLUMNS"'/;
            s/ORDER BY \(/ORDER BY ('"$EXTRA_ORDER_BY_COLUMNS"'/;
            s/^CREATE TABLE system\.\w+_log$/CREATE TABLE IF NOT EXISTS '"$table"'_'"$hash"'/;
            /^TTL /d
            ')

        echo -e "Creating remote destination table ${table}_${hash} with statement:\n${statement}" >&2

        echo "$statement" | clickhouse-client --database_replicated_initial_query_timeout_sec=10 \
            --distributed_ddl_task_timeout=30 \
            "${CONNECTION_ARGS[@]}" || continue

        echo "Creating table system.${table}_sender" >&2

        # Create Distributed table and materialized view to watch on the original table:
        clickhouse-client --query "
            CREATE TABLE system.${table}_sender
            ENGINE = Distributed(${CLICKHOUSE_CI_LOGS_CLUSTER}, default, ${table}_${hash})
            SETTINGS flush_on_detach=0
            EMPTY AS
            SELECT ${EXTRA_COLUMNS_EXPRESSION}, *
            FROM system.${table}
        " || continue

        echo "Creating materialized view system.${table}_watcher" >&2

        clickhouse-client --query "
            CREATE MATERIALIZED VIEW system.${table}_watcher TO system.${table}_sender AS
            SELECT ${EXTRA_COLUMNS_EXPRESSION}, *
            FROM system.${table}
        " || continue
    done
)
