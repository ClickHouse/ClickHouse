#!/bin/bash

set -e
# This script sets up export of system log tables to a remote server.
# Remote tables are created if not exist, and augmented with extra columns,
# and their names will contain a hash of the table structure,
# which allows exporting tables from servers of different versions.

# Config file contains KEY=VALUE pairs with any necessary parameters like:
# CLICKHOUSE_CI_LOGS_HOST - remote host
# CLICKHOUSE_CI_LOGS_USER - password for user
# CLICKHOUSE_CI_LOGS_PASSWORD - password for user

# Pre-configured destination cluster, where to export the data
CLICKHOUSE_CI_LOGS_CLUSTER=${CLICKHOUSE_CI_LOGS_CLUSTER:-system_logs_export}

EXTRA_COLUMNS=${EXTRA_COLUMNS:-"repo LowCardinality(String), pull_request_number UInt32, commit_sha String, check_start_time DateTime('UTC'), check_name LowCardinality(String), instance_type LowCardinality(String), instance_id String, INDEX ix_repo (repo) TYPE set(100), INDEX ix_pr (pull_request_number) TYPE set(100), INDEX ix_commit (commit_sha) TYPE set(100), INDEX ix_check_time (check_start_time) TYPE minmax, "}
echo "EXTRA_COLUMNS_EXPRESSION=${EXTRA_COLUMNS_EXPRESSION:?}"
EXTRA_ORDER_BY_COLUMNS=${EXTRA_ORDER_BY_COLUMNS:-"check_name"}

function __set_connection_args()
{
    # It's impossible to use a generic $CONNECTION_ARGS string, it's unsafe from word splitting perspective.
    # That's why we must stick to the generated option
    CONNECTION_ARGS=(
        --receive_timeout=45 --send_timeout=45 --secure
        --user "${CLICKHOUSE_CI_LOGS_USER:?}" --host "${CLICKHOUSE_CI_LOGS_HOST:?}"
        --password "${CLICKHOUSE_CI_LOGS_PASSWORD:?}"
    )
}

function __shadow_credentials()
{
    # The function completely screws the output, it shouldn't be used in normal functions, only in ()
    # The only way to substitute the env as a plain text is using perl 's/\Qsomething\E/another/
    exec &> >(perl -pe '
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_HOST}\E)[CLICKHOUSE_CI_LOGS_HOST]g;
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_USER}\E)[CLICKHOUSE_CI_LOGS_USER]g;
        s(\Q$ENV{CLICKHOUSE_CI_LOGS_PASSWORD}\E)[CLICKHOUSE_CI_LOGS_PASSWORD]g;
    ')
}

function check_logs_credentials()
{
    # The function connects with given credentials, and if it's unable to execute the simplest query, returns exit code

    set +x
    echo "Check CI Log cluster..."
    __set_connection_args
    __shadow_credentials
    local code
    # Catch both success and error to not fail on `set -e`
    clickhouse-client "${CONNECTION_ARGS[@]:?}" -q 'SELECT 1 FORMAT Null' && return 0 || code=$?
    if [ "$code" != 0 ]; then
        echo 'Failed to connect to CI Logs cluster'
        return $code
    fi
}

function setup_logs_replication()
{
    # The function is launched in a separate shell instance to not expose the
    # exported values
    set +x

    if [[ -n "$CLICKHOUSE_CI_LOGS_HOST" ]]; then
        check_logs_credentials
    else
        echo 'No CI logs creds found, tables check will be skipped'
    fi

    echo "My hostname is ${HOSTNAME}"

    echo 'Create all configured system logs'
    clickhouse-client --query "SYSTEM FLUSH LOGS"

    debug_or_sanitizer_build=$(clickhouse-client -q "WITH ((SELECT value FROM system.build_options WHERE name='BUILD_TYPE') AS build, (SELECT value FROM system.build_options WHERE name='CXX_FLAGS') as flags) SELECT build='Debug' OR flags LIKE '%fsanitize%'")
    echo "Build is debug or sanitizer: $debug_or_sanitizer_build"

    # For each system log table:
    echo 'Create %_log tables'
    clickhouse-client --query "SHOW TABLES FROM system LIKE '%\\_log'" | while read -r table
    do
        EXTRA_COLUMNS_FOR_TABLE="${EXTRA_COLUMNS}"
        EXTRA_COLUMNS_EXPRESSION_FOR_TABLE="${EXTRA_COLUMNS_EXPRESSION}"

        # Calculate hash of its structure according to the columns and their types, including extra columns
        hash=$(clickhouse-client --query "
            SELECT sipHash64(groupArray((SELECT columns FROM external)), groupArray((name, type)))
            FROM (SELECT name, type FROM system.columns
                WHERE database = 'system' AND table = '$table'
                ORDER BY position)
            " --external --name=external --file=- --structure='columns String' <<< "$EXTRA_COLUMNS_FOR_TABLE"
        )

        # Create the destination table with adapted name and structure:
        statement=$(clickhouse-client --format TSVRaw --query "SHOW CREATE TABLE system.${table}" | sed -r -e '
            s/^\($/('"$EXTRA_COLUMNS_FOR_TABLE"'/;
            s/^ORDER BY (([^\(].+?)|\((.+?)\))$/ORDER BY ('"$EXTRA_ORDER_BY_COLUMNS"', \2\3)/;
            s/^CREATE TABLE system\.\w+_log$/CREATE TABLE IF NOT EXISTS '"$table"'_'"$hash"'/;
            /^TTL /d
            /^SETTINGS /d
            /^COMMENT /d
            ')
        statement+=" SETTINGS use_const_adaptive_granularity = 1"

        echo -e "Creating remote destination table ${table}_${hash} with statement:" >&2

        echo "::group::${table}"
        # there's the only way big "$statement" can be printed without causing EAGAIN error
        # cat: write error: Resource temporarily unavailable
        statement_print="${statement}"
        if [ "${#statement_print}" -gt 4000 ]; then
          statement_print="${statement::1999}\n…\n${statement:${#statement}-1999}"
        fi
        echo -e "$statement_print"
        echo "::endgroup::"

        echo "$statement" | clickhouse-client --database_replicated_initial_query_timeout_sec=10 \
            --distributed_ddl_task_timeout=30 --distributed_ddl_output_mode=throw_only_active \
            "${CONNECTION_ARGS[@]:?}" || continue

        echo "Creating table system.${table}_sender" >&2

        # Create Distributed table and materialized view to watch on the original table:
        clickhouse-client --query "
            CREATE TABLE system.${table}_sender
            ENGINE = Distributed(${CLICKHOUSE_CI_LOGS_CLUSTER}, default, ${table}_${hash})
            SETTINGS flush_on_detach=0
            EMPTY AS
            SELECT ${EXTRA_COLUMNS_EXPRESSION_FOR_TABLE}, *
            FROM system.${table}
        " || continue

        echo "Creating materialized view system.${table}_watcher" >&2

        clickhouse-client --query "
            CREATE MATERIALIZED VIEW system.${table}_watcher
            TO system.${table}_sender
            DEFINER = ci_logs_sender
            AS
            SELECT ${EXTRA_COLUMNS_EXPRESSION_FOR_TABLE}, * FROM system.${table}
        " || continue
    done
}

function stop_logs_replication()
{
    echo "Detach all logs replication"
    clickhouse-client --query "select database||'.'||table from system.tables where database = 'system' and (table like '%_sender' or table like '%_watcher')" | {
        tee /dev/stderr
    } | {
        timeout --verbose --preserve-status --signal TERM --kill-after 5m 15m xargs -n1 -P10 -r -i clickhouse-client --query "drop table {}"
    }
}


while [[ "$#" -gt 0 ]]; do
    case $1 in
        --stop-log-replication)
            echo "Stopping log replication..."
            stop_logs_replication
            ;;
        --setup-logs-replication)
            echo "Setting up log replication..."
            setup_logs_replication
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--stop-log-replication | --setup-logs-replication ]"
            exit 1
            ;;
    esac
    shift
done
