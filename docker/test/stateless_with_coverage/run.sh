#!/bin/bash

kill_clickhouse () {
    echo "clickhouse pids" `ps aux | grep clickhouse` | ts '%Y-%m-%d %H:%M:%S'
    kill `pgrep -u clickhouse` 2>/dev/null

    for i in {1..10}
    do
        if ! kill -0 `pgrep -u clickhouse`; then
            echo "No clickhouse process" | ts '%Y-%m-%d %H:%M:%S'
            break
        else
            echo "Process" `pgrep -u clickhouse` "still alive" | ts '%Y-%m-%d %H:%M:%S'
            sleep 10
        fi
    done

    echo "Will try to send second kill signal for sure"
    kill `pgrep -u clickhouse` 2>/dev/null
    sleep 5
    echo "clickhouse pids" `ps aux | grep clickhouse` | ts '%Y-%m-%d %H:%M:%S'
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

# Temorary way to keep CI green while moving dictionaries to separate directory
mkdir -p /etc/clickhouse-server/dict_examples
chmod 777 -R /etc/clickhouse-server/dict_examples
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/;

ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/access_management.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/macros.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/disks.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/secure_ports.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/clusters.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/graphite.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/server.key /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/server.crt /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/dhparam.pem /etc/clickhouse-server/
ln -sf /usr/share/clickhouse-test/config/client_config.xml /etc/clickhouse-client/config.xml

# Retain any pre-existing config and allow ClickHouse to load it if required
ln -s --backup=simple --suffix=_original.xml \
    /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/

service zookeeper start
sleep 5

start_clickhouse

sleep 10


if cat /usr/bin/clickhouse-test | grep -q -- "--use-skip-list"; then
    SKIP_LIST_OPT="--use-skip-list"
fi

LLVM_PROFILE_FILE='client_coverage.profraw' clickhouse-test --testname --shard --zookeeper "$SKIP_LIST_OPT" $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt

kill_clickhouse

sleep 3

cp /*.profraw /profraw ||:
