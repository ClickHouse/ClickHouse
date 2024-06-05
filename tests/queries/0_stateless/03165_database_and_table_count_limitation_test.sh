#!/usr/bin/env bash

#Tags: shared, no-parallel

# Get the current script directory
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BASE="$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh)"

# Load shell_config.sh
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# remove --database=$CLICKHOUSE_DATABASE from $CLICKHOUSE_CLIENT
CLICKHOUSE_CLIENT=$(echo $CLICKHOUSE_CLIENT | sed "s/--database=$CLICKHOUSE_DATABASE//")

# Create a temporary directory
TEMP_DIR=$(mktemp -d "$CUR_DIR/$(basename "$BASE").XXXXXX")

# Generate config.xml
CONFIG_FILE="$TEMP_DIR/config.xml"
cat > "$CONFIG_FILE" <<EOL
<yandex>
    <logger>
        <level>information</level>
        <log>$TEMP_DIR/clickhouse-server.log</log>
        <errorlog>$TEMP_DIR/clickhouse-server.err.log</errorlog>
    </logger>
    <max_table_num_to_throw>10</max_table_num_to_throw>
    <max_database_num_to_throw>10</max_database_num_to_throw>

    <user_directories>
        <users_xml>
            <!-- Path to configuration file with predefined users. -->
            <path>users.xml</path>
        </users_xml>
    </user_directories>

</yandex>
EOL

echo        "<clickhouse>
            <profiles>
                <default></default>
            </profiles>
            <users>
                <default>
                    <password></password>
                    <networks>
                        <ip>::/0</ip>
                    </networks>
                    <profile>default</profile>
                    <quota>default</quota>
                </default>
            </users>
            <quotas>
                <default></default>
            </quotas>
        </clickhouse>" > $TEMP_DIR/users.xml

# Function to start the server
function start_server() {
    local server_opts=(
        "--config-file=$CONFIG_FILE"
        "--"
        "--tcp_port=0"
        "--shutdown_wait_unfinished=0"
        "--listen_host=127.1"
        "--path=$TEMP_DIR"
    )
    CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" > /dev/null 2>&1 &
    local pid=$!

    echo "$pid"
}

# Function to get the server port
function get_server_port() {
    local pid=$1 && shift
    local port=''
    while [[ -z $port ]]; do
        port=$(lsof -n -a -P -i tcp -s tcp:LISTEN -p "$pid" 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1) }')
        sleep 0.5
    done
    echo "$port"
}

# Function to wait for the server port to be available
function wait_server_port() {
    local port=$1 && shift
    local i=0 retries=30
    while ! $CLICKHOUSE_CLIENT --host 127.1 --port "$port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
        sleep 0.5
        ((i++))
    done
    if ! $CLICKHOUSE_CLIENT --host 127.1 --port "$port" --format Null -q 'select 1'; then
        echo "Cannot wait until server will start accepting connections on port $port" >&2
        exit 1
    fi
}

# Function to stop the server
function stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        kill -9 "$SERVER_PID"
    fi
}

# Function to clean up
function cleanup() {
    stop_server
    rm -rf "$TEMP_DIR"
}

trap cleanup EXIT

# Start the server and get the port
SERVER_PID=$(start_server)
PORT=$(get_server_port "$SERVER_PID")

# Wait for the server to start
wait_server_port "$PORT"

# check result
sql_file="$CUR_DIR/03165_database_and_table_count_limitation_sql"
result_file="$CUR_DIR/03165_database_and_table_count_limitation_result"
reference_file="$CUR_DIR/03165_database_and_table_count_limitation_reference"

$CLICKHOUSE_CLIENT --host 127.1 --port "$PORT" --multiquery --ignore-error --queries-file=$sql_file 2>/dev/null > "$result_file"

# Compare the result with the reference, if not same, print the diff
if ! diff -u "$reference_file" "$result_file"; then
    echo "Test failed"
    exit 1
fi

# check errors in error log
log_file="$TEMP_DIR/clickhouse-server.err.log"
database_error=$(grep -c "<Error> executeQuery: Code: 725. DB::Exception: Too many databases" $log_file)
table_error=$(grep -c " <Error> executeQuery: Code: 724. DB::Exception: Too many tables" $log_file)
#database_error should be 2
if [ $database_error -ne 2 ]; then
    echo "database_error should be 2, but now is $database_error. Tried to create 8 db, 6 should be created and 2 should fail"
    echo "Limitation is 10 databases, 4 exist by default: default, system, information_schema, INFORMATION_SCHEMA"
    exit 1
fi

# table_error should be 1
if [ $table_error -ne 1 ]; then
    echo "table_error should be 1, but now -s $table_error. Tried to create 11 tables, 10 should be created and 1 should fail"
    echo "Limitation is 10 tables"
    exit 1
fi

echo "Test passed"
