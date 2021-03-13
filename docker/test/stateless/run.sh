#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

# install test configs
/usr/share/clickhouse-test/config/install.sh
#cp /use_test_keeper.xml /etc/clickhouse-server/config.d/zookeeper.xml
#cp /enable_test_keeper1.xml /etc/clickhouse-server/config.d/test_keeper_port.xml
#cp /clusters2.xml /etc/clickhouse-server/config.d/

#mkdir /etc/clickhouse-server2
#chown clickhouse /etc/clickhouse-server2
#chgrp clickhouse /etc/clickhouse-server2
#sudo -u clickhouse cp -r /etc/clickhouse-server/* /etc/clickhouse-server2
#rm /etc/clickhouse-server2/config.d/macros.xml
#sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<replica>r1</replica>|<replica>r2</replica>|" > /etc/clickhouse-server2/config.d/macros.xml

#cat /usr/bin/clickhouse-test | sed "s| ENGINE=Replicated('/test/clickhouse/db/{}', 's1', 'r1')| ON CLUSTER test_cluster_database_replicated ENGINE=Replicated('/test/clickhouse/db/{}', '{{shard}}', '{{replica}}')|" > /usr/bin/clickhouse-test-tmp
#mv /usr/bin/clickhouse-test-tmp /usr/bin/clickhouse-test
#chmod a+x /usr/bin/clickhouse-test

# For flaky check we also enable thread fuzzer
if [ "$NUM_TRIES" -gt "1" ]; then
    export THREAD_FUZZER_CPU_TIME_PERIOD_US=1000
    export THREAD_FUZZER_SLEEP_PROBABILITY=0.1
    export THREAD_FUZZER_SLEEP_TIME_US=100000

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY=1

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US=10000
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US=10000

    # simpliest way to forward env variables to server
    sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon
else
    service clickhouse-server start
fi

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    # There is a bug in config reloading, so we cannot override macros using --macros.replica r2
    # And we have to copy configs...
    mkdir /etc/clickhouse-server2
    chown clickhouse /etc/clickhouse-server2
    chgrp clickhouse /etc/clickhouse-server2
    sudo -u clickhouse cp -r /etc/clickhouse-server/* /etc/clickhouse-server2
    rm /etc/clickhouse-server2/config.d/macros.xml
    sudo -u clickhouse cat /etc/clickhouse-server/config.d/macros.xml | sed "s|<replica>r1</replica>|<replica>r2</replica>|" > /etc/clickhouse-server2/config.d/macros.xml

    sudo mkdir /var/lib/clickhouse2
    sudo chmod a=rwx /var/lib/clickhouse2
    sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server2/config.xml --daemon \
    -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
    --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
    --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
    --mysql_port 19004 \
    --test_keeper_server.tcp_port 19181 --test_keeper_server.server_id 2 \
    --macros.replica r2   # It doesn't work :(
fi

sleep 5

if grep -q -- "--use-skip-list" /usr/bin/clickhouse-test; then
    SKIP_LIST_OPT="--use-skip-list"
fi

function run_tests()
{
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

    # Skip these tests, because they fail when we rerun them multiple times
    if [ "$NUM_TRIES" -gt "1" ]; then
        ADDITIONAL_OPTIONS+=('--skip')
        ADDITIONAL_OPTIONS+=('00000_no_tests_to_skip')
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('4')
    fi

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
    fi

    clickhouse-test --testname --shard --zookeeper --hung-check --print-time \
            --test-runs "$NUM_TRIES" \
            "$SKIP_LIST_OPT" "${ADDITIONAL_OPTIONS[@]}" 2>&1 \
        | ts '%Y-%m-%d %H:%M:%S' \
        | tee -a test_output/test_result.txt
}

#clickhouse-client --port 9000 -q "SELECT * FROM system.macros"
#clickhouse-client --port 19000 -q "SELECT * FROM system.macros"
#clickhouse-client --port 19000 -q "SELECT 2"

export -f run_tests

timeout "$MAX_RUN_TIME" bash -c run_tests ||:

./process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

pigz < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.gz ||:
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:
if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar -chf /test_output/clickhouse_coverage.tar.gz /profraw ||:
fi
tar -chf /test_output/text_log_dump.tar /var/lib/clickhouse/data/system/text_log ||:
tar -chf /test_output/query_log_dump.tar /var/lib/clickhouse/data/system/query_log ||:

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    pigz < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.gz ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
fi
