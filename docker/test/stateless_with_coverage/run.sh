#!/bin/bash

kill_clickhouse () {
    echo "clickhouse pids $(pgrep -u clickhouse)" | ts '%Y-%m-%d %H:%M:%S'
    kill "$(pgrep -u clickhouse)" 2>/dev/null

    for _ in {1..10}
    do
        if ! kill -0 "$(pgrep -u clickhouse)"; then
            echo "No clickhouse process" | ts '%Y-%m-%d %H:%M:%S'
            break
        else
            echo "Process $(pgrep -u clickhouse) still alive" | ts '%Y-%m-%d %H:%M:%S'
            sleep 10
        fi
    done

    echo "Will try to send second kill signal for sure"
    kill "$(pgrep -u clickhouse)" 2>/dev/null
    sleep 5
    echo "clickhouse pids $(pgrep -u clickhouse)" | ts '%Y-%m-%d %H:%M:%S'
}

start_clickhouse () {
    LLVM_PROFILE_FILE='server_%h_%p_%m.profraw' sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml &
}

chmod 777 /

dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb


mkdir -p /var/lib/clickhouse
mkdir -p /var/log/clickhouse-server
chmod 777 -R /var/lib/clickhouse
chmod 777 -R /var/log/clickhouse-server/

# install test configs
/usr/share/clickhouse-test/config/install.sh

start_clickhouse

sleep 10


if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-test --testname --shard --zookeeper "$SKIP_LIST_OPT" "$ADDITIONAL_OPTIONS" "$SKIP_TESTS_OPTION" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt

kill_clickhouse

sleep 3

cp /*.profraw /profraw ||:
