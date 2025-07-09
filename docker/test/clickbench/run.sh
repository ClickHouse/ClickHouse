#!/bin/bash

SCRIPT_PID=$!
(sleep 1200 && kill -9 $SCRIPT_PID) &

# shellcheck disable=SC1091
source /setup_export_logs.sh

# fail on errors, verbose and export all env variables
set -e -x -a

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

# A directory for cache
mkdir /dev/shm/clickhouse
chown clickhouse:clickhouse /dev/shm/clickhouse

# Allow introspection functions, needed for sending the logs
echo "
profiles:
    default:
        allow_introspection_functions: 1
" > /etc/clickhouse-server/users.d/allow_introspection_functions.yaml

# Enable text_log
echo "
text_log:
" > /etc/clickhouse-server/config.d/text_log.yaml

config_logs_export_cluster /etc/clickhouse-server/config.d/system_logs_export.yaml

clickhouse start

# Wait for the server to start, but not for too long.
for _ in {1..100}
do
    clickhouse-client --query "SELECT 1" && break
    sleep 1
done

setup_logs_replication

# Load the data

clickhouse-client --time < /create.sql

# Run the queries

set +x

TRIES=3
QUERY_NUM=1
while read -r query; do
    echo -n "["
    for i in $(seq 1 $TRIES); do
        RES=$(clickhouse-client --query_id "q${QUERY_NUM}-${i}" --time --format Null --query "$query" --progress 0 2>&1 ||:)
        echo -n "${RES}"
        [[ "$i" != "$TRIES" ]] && echo -n ", "

        echo "${QUERY_NUM},${i},${RES}" >> /test_output/test_results.tsv
    done
    echo "],"

    QUERY_NUM=$((QUERY_NUM + 1))
done < /queries.sql

set -x

clickhouse-client --query "SELECT total_bytes FROM system.tables WHERE name = 'hits' AND database = 'default'"

clickhouse-client -q "system flush logs" ||:
stop_logs_replication
clickhouse stop

mv /var/log/clickhouse-server/* /test_output/

echo -e "success\tClickBench finished" > /test_output/check_status.tsv
