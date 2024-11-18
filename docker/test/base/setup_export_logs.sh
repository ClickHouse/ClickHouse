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

EXTRA_COLUMNS=${EXTRA_COLUMNS:-"pull_request_number UInt32, commit_sha String, check_start_time DateTime('UTC'), check_name LowCardinality(String), instance_type LowCardinality(String), instance_id String, INDEX ix_pr (pull_request_number) TYPE set(100), INDEX ix_commit (commit_sha) TYPE set(100), INDEX ix_check_time (check_start_time) TYPE minmax, "}
EXTRA_COLUMNS_EXPRESSION=${EXTRA_COLUMNS_EXPRESSION:-"CAST(0 AS UInt32) AS pull_request_number, '' AS commit_sha, now() AS check_start_time, toLowCardinality('') AS check_name, toLowCardinality('') AS instance_type, '' AS instance_id"}
EXTRA_ORDER_BY_COLUMNS=${EXTRA_ORDER_BY_COLUMNS:-"check_name"}

# trace_log needs more columns for symbolization
EXTRA_COLUMNS_TRACE_LOG="${EXTRA_COLUMNS} symbols Array(LowCardinality(String)), lines Array(LowCardinality(String)), "
EXTRA_COLUMNS_EXPRESSION_TRACE_LOG="${EXTRA_COLUMNS_EXPRESSION}, arrayMap(x -> demangle(addressToSymbol(x)), trace)::Array(LowCardinality(String)) AS symbols, arrayMap(x -> addressToLine(x), trace)::Array(LowCardinality(String)) AS lines"

# coverage_log needs more columns for symbolization, but only symbol names (the line numbers are too heavy to calculate)
EXTRA_COLUMNS_COVERAGE_LOG="${EXTRA_COLUMNS} symbols Array(LowCardinality(String)), "
EXTRA_COLUMNS_EXPRESSION_COVERAGE_LOG="${EXTRA_COLUMNS_EXPRESSION}, arrayDistinct(arrayMap(x -> demangle(addressToSymbol(x)), coverage))::Array(LowCardinality(String)) AS symbols"


function __set_connection_args
{
    # It's impossible to use a generic $CONNECTION_ARGS string, it's unsafe from word splitting perspective.
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

    echo "My hostname is ${HOSTNAME}"

    echo 'Create all configured system logs'
    clickhouse-client --query "SYSTEM FLUSH LOGS"

    debug_or_sanitizer_build=$(clickhouse-client -q "WITH ((SELECT value FROM system.build_options WHERE name='BUILD_TYPE') AS build, (SELECT value FROM system.build_options WHERE name='CXX_FLAGS') as flags) SELECT build='Debug' OR flags LIKE '%fsanitize%'")
    echo "Build is debug or sanitizer: $debug_or_sanitizer_build"

    # We will pre-create a table system.coverage_log.
    # It is normally created by clickhouse-test rather than the server,
    # so we will create it in advance to make it be picked up by the next commands:

    clickhouse-client --query "
        CREATE TABLE IF NOT EXISTS system.coverage_log
        (
            time DateTime COMMENT 'The time of test run',
            test_name String COMMENT 'The name of the test',
            coverage Array(UInt64) COMMENT 'An array of addresses of the code (a subset of addresses instrumented for coverage) that were encountered during the test run'
        ) ENGINE = MergeTree ORDER BY test_name COMMENT 'Contains information about per-test coverage from the CI, but used only for exporting to the CI cluster'
    "

    # For each system log table:
    echo 'Create %_log tables'
    clickhouse-client --query "SHOW TABLES FROM system LIKE '%\\_log'" | while read -r table
    do
        if [[ "$table" = "trace_log" ]]
        then
            EXTRA_COLUMNS_FOR_TABLE="${EXTRA_COLUMNS_TRACE_LOG}"
            # Do not try to resolve stack traces in case of debug/sanitizers
            # build, since it is too slow (flushing of trace_log can take ~1min
            # with such MV attached)
            if [[ "$debug_or_sanitizer_build" = 1 ]]
            then
                EXTRA_COLUMNS_EXPRESSION_FOR_TABLE="${EXTRA_COLUMNS_EXPRESSION}"
            else
                EXTRA_COLUMNS_EXPRESSION_FOR_TABLE="${EXTRA_COLUMNS_EXPRESSION_TRACE_LOG}"
            fi
        elif [[ "$table" = "coverage_log" ]]
        then
            EXTRA_COLUMNS_FOR_TABLE="${EXTRA_COLUMNS_COVERAGE_LOG}"
            EXTRA_COLUMNS_EXPRESSION_FOR_TABLE="${EXTRA_COLUMNS_EXPRESSION_COVERAGE_LOG}"
        else
            EXTRA_COLUMNS_FOR_TABLE="${EXTRA_COLUMNS}"
            EXTRA_COLUMNS_EXPRESSION_FOR_TABLE="${EXTRA_COLUMNS_EXPRESSION}"
        fi

        # Calculate hash of its structure. Note: 4 is the version of extra columns - increment it if extra columns are changed:
        hash=$(clickhouse-client --query "
            SELECT sipHash64(9, groupArray((name, type)))
            FROM (SELECT name, type FROM system.columns
                WHERE database = 'system' AND table = '$table'
                ORDER BY position)
            ")

        # Create the destination table with adapted name and structure:
        statement=$(clickhouse-client --format TSVRaw --query "SHOW CREATE TABLE system.${table}" | sed -r -e '
            s/^\($/('"$EXTRA_COLUMNS_FOR_TABLE"'/;
            s/^ORDER BY (([^\(].+?)|\((.+?)\))$/ORDER BY ('"$EXTRA_ORDER_BY_COLUMNS"', \2\3)/;
            s/^CREATE TABLE system\.\w+_log$/CREATE TABLE IF NOT EXISTS '"$table"'_'"$hash"'/;
            /^TTL /d
            ')

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
            "${CONNECTION_ARGS[@]}" || continue

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
            CREATE MATERIALIZED VIEW system.${table}_watcher TO system.${table}_sender AS
            SELECT ${EXTRA_COLUMNS_EXPRESSION_FOR_TABLE}, *
            FROM system.${table}
        " || continue
    done
)

function stop_logs_replication
{
    echo "Detach all logs replication"
    clickhouse-client --query "select database||'.'||table from system.tables where database = 'system' and (table like '%_sender' or table like '%_watcher')" | {
        tee /dev/stderr
    } | {
        timeout --preserve-status --signal TERM --kill-after 5m 15m xargs -n1 -r -i clickhouse-client --query "drop table {}"
    }
}
