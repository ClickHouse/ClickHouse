#!/bin/bash

set -x

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

function stop()
{
    timeout 120 service clickhouse-server stop

    # Wait for process to disappear from processlist and also try to kill zombies.
    while kill -9 $(pidof clickhouse-server)
    do
        echo "Killed clickhouse-server"
        sleep 0.5
    done
}

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

ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/

echo "TSAN_OPTIONS='halt_on_error=1 history_size=7 ignore_noninstrumented_modules=1 verbosity=1'" >> /etc/environment
echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment
echo "ASAN_OPTIONS='malloc_context_size=10 verbosity=1 allocator_release_to_os_interval_ms=10000'" >> /etc/environment

start

/s3downloader --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"

stop
start

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
clickhouse-client --query "SHOW TABLES FROM test"

./stress --output-folder test_output --skip-func-tests "$SKIP_TESTS_OPTION"

stop
start

clickhouse-client --query "SELECT 'Server successfuly started'" > /test_output/alive_check.txt || echo 'Server failed to start' > /test_output/alive_check.txt
