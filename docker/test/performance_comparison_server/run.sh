#!/bin/bash

set -ex

init_server()
{
    sudo -u clickhouse clickhouse-server --config-file /etc/clickhouse-server/config.xml -- --tcp_port $1 --http_port $2 --interserver_http_port $3 2>/var/log/clickhouse-server/stderr.log
}

first_run() # This run is made to create all the directories and create databases
{
    init_server $1 $2 $3 &
    sleep 5

    clickhouse-client --port $1 --query "CREATE DATABASE IF NOT EXISTS datasets"
    clickhouse-client --port $1 --query "CREATE DATABASE IF NOT EXISTS test"

    pkill -9 -f clickhouse-server
}

second_run() # This run is made to rename a table xD
{
    init_server $1 $2 $3 &
    sleep 5

    clickhouse-client --port $1 --query "DROP TABLE IF EXISTS test.hits"
    clickhouse-client --port $1 --query "RENAME TABLE datasets.hits_v1 TO test.hits"

    pkill -9 -f clickhouse-server
}

final_run() # Final run
{
    init_server $1 $2 $3
    sleep 5

}

install_packages()
{
    dpkg -i package_folder/clickhouse-common-static_*.deb
    dpkg -i package_folder/clickhouse-server_*.deb
    dpkg -i package_folder/clickhouse-client_*.deb
}

download_data()
{
    if [ $DOWNLOAD_DATASETS -eq 1 ]; then
      /s3downloader --dataset-names $OPEN_DATASETS
      /s3downloader --dataset-names $PRIVATE_DATASETS --url 'https://s3.mds.yandex.net/clickhouse-private-datasets'
    fi
}

chmod 777 /var/lib/clickhouse
chmod 777 /var/log/clickhouse-server

install_packages

first_run $(($PORT + 9000)) $(($PORT + 10000)) $(($PORT + 11000))

download_data

second_run $(($PORT + 9000)) $(($PORT + 10000)) $(($PORT + 11000))

final_run $(($PORT)) $(($PORT + 10000)) $(($PORT + 11000))