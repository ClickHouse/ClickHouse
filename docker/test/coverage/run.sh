#!/bin/bash

kill_clickhouse () {
    echo "clickhouse pids $(pgrep -u clickhouse)" | ts '%Y-%m-%d %H:%M:%S'
    pkill -f "clickhouse-server" 2>/dev/null

    for _ in {1..120}
    do
        if ! pkill -0 -f "clickhouse-server" ; then break ; fi
        echo "ClickHouse still alive" | ts '%Y-%m-%d %H:%M:%S'
        sleep 1
    done

    if pkill -0 -f "clickhouse-server"
    then
        pstree -apgT
        jobs
        echo "Failed to kill the ClickHouse server"  | ts '%Y-%m-%d %H:%M:%S'
        return 1
    fi
}

start_clickhouse () {
    sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml &
    counter=0

    until clickhouse-client --query "SELECT 1"
    do
        if [ "$counter" -gt 120 ]
        then
            echo "Cannot start clickhouse-server"
            cat /var/log/clickhouse-server/stdout.log
            tail -n1000 /var/log/clickhouse-server/stderr.log
            tail -n1000 /var/log/clickhouse-server/clickhouse-server.log
            break
        fi
        sleep 0.5
        counter=$((counter + 1))
    done
}

wail_till_ready () {
    echo "Waiting for server to symbolize all addresses"

    tail -n +1 -f /var/log/clickhouse-server/clickhouse-server.log | sed '/Symbolized all addresses/ q' > /dev/null

    echo "Symbolized all addresses"

    instrumented_contribs=$(grep "contrib/" < /report.ccr)
    has_contribs=$(echo "$instrumented_contribs" | wc -l)

    if ((has_contribs > 1)); then
        echo "Warning: found instrumented files from contrib/. Please remove them explicitly via cmake"
        echo "$instrumented_contribs"
    fi

    echo "Starting tests"
}


chmod 777 /

dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb

mkdir -p /var/lib/clickhouse
mkdir -p /var/log/clickhouse-server
chmod 777 -R /var/log/clickhouse-server/

# install test configs
/usr/share/clickhouse-test/config/install.sh

start_clickhouse

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
if ! /s3downloader --dataset-names $DATASETS; then
    echo "Cannot download datatsets"
    exit 1
fi

chmod 777 -R /var/lib/clickhouse

wail_till_ready

clickhouse-client --query "SHOW DATABASES"
clickhouse-client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE test"

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
clickhouse-client --query "SHOW TABLES FROM test"

# Skip tests that invoke clickhouse-server or any of other ClickHouse modes
# as only one binary instance (excluding clickhouse-client instances) is allowed to run.
clickhouse-test --testname --shard --zookeeper --print-time --use-skip-list --coverage \
    --skip 01737_clickhouse_server_wait_server_pool_long \
        01801_s3_cluster \
        01526_max_untracked_memory \
        01507_clickhouse_server_start_with_embedded_config \
        01086_odbc_roundtrip \
        00157_cache_dictionary \
    2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /test_result.txt

kill_clickhouse

cp /report.ccr "${OUTPUT_DIR}"/report.ccr

python3 ccr_converter.py /report.ccr "${OUTPUT_DIR}" "${SOURCE_DIR}"
