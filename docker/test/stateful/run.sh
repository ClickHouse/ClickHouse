#!/bin/bash

set -e -x

dpkg -i package_folder/clickhouse-common-static_*.deb;
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

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
