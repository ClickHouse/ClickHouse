#!/bin/bash

kill_clickhouse () {
    while kill -0 `pgrep -u clickhouse`;
    do
        kill `pgrep -u clickhouse` 2>/dev/null
        echo "Process" `pgrep -u clickhouse` "still alive"
        sleep 10
    done
}

wait_llvm_profdata () {
    while kill -0 `pgrep llvm-profdata-9`;
    do
        echo "Waiting for profdata " `pgrep llvm-profdata-9` "still alive"
        sleep 3
    done
}

start_clickhouse () {
    LLVM_PROFILE_FILE='server_%h_%p_%m.profraw' sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml &
}

merge_client_files_in_background () {
    client_files=`ls /client_*profraw 2>/dev/null`
    if [ ! -z "$client_files" ]
    then
        llvm-profdata-9 merge -sparse $client_files -o merged_client_`date +%s`.profraw
        rm $client_files
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
chmod 777 -R /var/lib/clickhouse
chmod 777 -R /var/log/clickhouse-server/

ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer

service zookeeper start
sleep 5

start_clickhouse

sleep 10

while /bin/true; do
    merge_client_files_in_background
    sleep 2
done &

LLVM_PROFILE_FILE='client_%h_%p_%m.profraw' clickhouse-test --shard --zookeeper $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt

kill_clickhouse

wait_llvm_profdata

sleep 3

wait_llvm_profdata # 100% merged all parts

cp /*.profraw /profraw ||:
