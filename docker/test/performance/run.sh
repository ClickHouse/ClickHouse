#!/bin/bash

set -ex

install_packages() {
    dpkg -i package_folder/clickhouse-common-static_*.deb
    dpkg -i package_folder/clickhouse-server_*.deb
    dpkg -i package_folder/clickhouse-client_*.deb
    dpkg -i package_folder/clickhouse-test_*.deb
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/
    service clickhouse-server start && sleep 5
}

download_data() {
    clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary"
    clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"
    /s3downloader --dataset-names $OPEN_DATASETS
    /s3downloader --dataset-names $PRIVATE_DATASETS --url 'https://s3.mds.yandex.net/clickhouse-private-datasets'
    chmod 777 -R /var/lib/clickhouse
    service clickhouse-server restart && sleep 5
    clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
}

run() {
    clickhouse-performance-test $TESTS_TO_RUN | tee test_output/test_result.json
}

install_packages

if [ $DOWNLOAD_DATASETS -eq 1 ]; then
    download_data
fi

clickhouse-client --query "select * from system.settings where name = 'log_queries'"
tree /etc/clickhouse-server
cat /var/lib/clickhouse/preprocessed_configs/config.xml
cat /var/lib/clickhouse/preprocessed_configs/users.xml

run
