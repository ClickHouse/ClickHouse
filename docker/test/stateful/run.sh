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

if [[ -n "$USE_DATABASE_ATOMIC" ]] && [[ "$USE_DATABASE_ATOMIC" -eq 1 ]]; then
    ln -s /usr/share/clickhouse-test/config/database_atomic_configd.xml /etc/clickhouse-server/config.d/
    ln -s /usr/share/clickhouse-test/config/database_atomic_usersd.xml /etc/clickhouse-server/users.d/
fi

echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment
echo "TSAN_SYMBOLIZER_PATH=/usr/lib/llvm-10/bin/llvm-symbolizer" >> /etc/environment
echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment
echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-10/bin/llvm-symbolizer" >> /etc/environment
echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-10/bin/llvm-symbolizer" >> /etc/environment
echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-10/bin/llvm-symbolizer" >> /etc/environment

service zookeeper start
sleep 5
service clickhouse-server start
sleep 5
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
