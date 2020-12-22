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
    LLVM_PROFILE_FILE='server_%h_%p_%m.profraw' sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml &
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


LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "SHOW DATABASES"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "CREATE DATABASE test"

kill_clickhouse
start_clickhouse

LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "SHOW TABLES FROM datasets"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "SHOW TABLES FROM test"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-client --query "SHOW TABLES FROM test"


if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

# We can have several additional options so we path them as array because it's
# more idiologically correct.
read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-test --testname --shard --zookeeper --no-stateless --hung-check --print-time "$SKIP_LIST_OPT" "${ADDITIONAL_OPTIONS[@]}" "$SKIP_TESTS_OPTION" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt

kill_clickhouse

sleep 3

cp /*.profraw /profraw ||:
