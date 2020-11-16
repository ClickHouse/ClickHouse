#!/bin/bash

kill_clickhouse () {
    kill "$(pgrep -u clickhouse)" 2>/dev/null

    for _ in {1..10}
    do
        if ! kill -0 "$(pgrep -u clickhouse)"; then
            echo "No clickhouse process"
            break
        else
            echo "Process $(pgrep -u clickhouse) still alive"
            sleep 10
        fi
    done
}

start_clickhouse () {
    LLVM_PROFILE_FILE='server_%h_%p_%m.profraw' sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml &
}

wait_llvm_profdata () {
    while kill -0 "$(pgrep llvm-profdata-10)"
    do
        echo "Waiting for profdata $(pgrep llvm-profdata-10) still alive"
        sleep 3
    done
}

merge_client_files_in_background () {
    client_files=$(ls /client_*profraw 2>/dev/null)
    if [ -n "$client_files" ]
    then
        llvm-profdata-10 merge -sparse "$client_files" -o "merged_client_$(date +%s).profraw"
        rm "$client_files"
    fi
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

function start()
{
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
        timeout 120 service clickhouse-server start
        sleep 0.5
        counter=$((counter + 1))
    done
}

start

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
if ! /s3downloader --dataset-names $DATASETS; then
    echo "Cannot download datatsets"
    exit 1
fi


chmod 777 -R /var/lib/clickhouse

while /bin/true; do
    merge_client_files_in_background
    sleep 2
done &

LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "SHOW DATABASES"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "CREATE DATABASE test"

kill_clickhouse
start_clickhouse

sleep 10

LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "SHOW TABLES FROM datasets"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "SHOW TABLES FROM test"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-client --query "SHOW TABLES FROM test"

if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-test --testname --shard --zookeeper --no-stateless "$SKIP_LIST_OPT" "$ADDITIONAL_OPTIONS" "$SKIP_TESTS_OPTION" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt

kill_clickhouse

wait_llvm_profdata

sleep 3

wait_llvm_profdata # 100% merged all parts


cp /*.profraw /profraw ||:
