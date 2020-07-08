#!/bin/bash

set -x -e

ls -la

git clone https://github.com/ClickHouse/ClickHouse.git | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/clone_log.txt
cd ClickHouse
CLICKHOUSE_DIR=`pwd`


if [ "$PULL_REQUEST_NUMBER" != "0" ]; then
    if git fetch origin "+refs/pull/$PULL_REQUEST_NUMBER/merge"; then
        git checkout FETCH_HEAD
        echo 'Clonned merge head'
    else
        git fetch
        git checkout $COMMIT_SHA
        echo 'Checked out to commit'
    fi
else
    if [ "$COMMIT_SHA" != "" ]; then
        git checkout $COMMIT_SHA
    fi
fi



git submodule update --init --recursive | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/submodule_log.txt

CMAKE_LIBS_CONFIG="-DENABLE_RDKAFKA=0 -DENABLE_S3=0 -DUSE_SENTRY=0 -DENABLE_AMQPCPP=0 -DENABLE_HDFS=0 -DENABLE_MYSQL=0 -DENABLE_GRPC=0 -DENABLE_CURL=0 -DENABLE_EMBEDDED_COMPILER=0"

export CCACHE_DIR=/ccache
export CCACHE_BASEDIR=/ClickHouse
export CCACHE_NOHASHDIR=true
export CCACHE_COMPILERCHECK=content
export CCACHE_MAXSIZE=15G

ccache --show-stats ||:
ccache --zero-stats ||:

mkdir build
cd build
CLICKHOUSE_BUILD_DIR=`pwd`
cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_CXX_COMPILER=clang++-10 -DCMAKE_C_COMPILER=clang-10 $CMAKE_LIBS_CONFIG | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/cmake_log.txt
ninja | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/build_log.txt
ninja install | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/install_log.txt


ccache --show-stats ||:

mkdir -p /etc/clickhouse-server
mkdir -p /etc/clickhouse-client
mkdir -p /etc/clickhouse-server/config.d
mkdir -p /etc/clickhouse-server/users.d
mkdir -p /var/log/clickhouse-server
cp $CLICKHOUSE_DIR/programs/server/config.xml /etc/clickhouse-server/
cp $CLICKHOUSE_DIR/programs/server/users.xml /etc/clickhouse-server/

mkdir -p /etc/clickhouse-server/dict_examples
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/
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

clickhouse-server --config /etc/clickhouse-server/config.xml --daemon

until clickhouse-client --query "SELECT 1"
do
    sleep 0.1
done

clickhouse-test --testname --shard --zookeeper 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/test_log.txt

mv /var/log/clickhouse-server/* /test_output
