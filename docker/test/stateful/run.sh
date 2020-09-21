#!/bin/bash

set -e -x

dpkg -i package_folder/clickhouse-common-static_*.deb;
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

mkdir -p /etc/clickhouse-server/dict_examples
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/macros.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/database_atomic_configd.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/database_atomic_usersd.xml /etc/clickhouse-server/users.d/

if [[ -n "$USE_DATABASE_ATOMIC" ]] && [[ "$USE_DATABASE_ATOMIC" -eq 1 ]]; then  #FIXME USE_DATABASE_ORDINARY
    ln -s /usr/share/clickhouse-test/config/database_ordinary_configd.xml /etc/clickhouse-server/config.d/
fi

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
        counter=$(($counter + 1))
    done
}

start
/s3downloader --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "SHOW DATABASES"
clickhouse-client --query "ATTACH DATABASE datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE test"
service clickhouse-server restart && sleep 5
clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
clickhouse-client --query "SHOW TABLES FROM test"

if cat /usr/bin/clickhouse-test | grep -q -- "--use-skip-list"; then
    SKIP_LIST_OPT="--use-skip-list"
fi

clickhouse-test --testname --shard --zookeeper --no-stateless "$SKIP_LIST_OPT" $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
