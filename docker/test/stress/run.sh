#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086

set -x

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb
dpkg -i package_folder/clickhouse-test_*.deb

function configure()
{
    # install test configs
    /usr/share/clickhouse-test/config/install.sh

    # for clickhouse-server (via service)
    echo "ASAN_OPTIONS='malloc_context_size=10 verbosity=1 allocator_release_to_os_interval_ms=10000'" >> /etc/environment
    # for clickhouse-client
    export ASAN_OPTIONS='malloc_context_size=10 allocator_release_to_os_interval_ms=10000'

    # since we run clickhouse from root
    sudo chown root: /var/lib/clickhouse

    # Set more frequent update period of asynchronous metrics to more frequently update information about real memory usage (less chance of OOM).
    echo "<yandex><asynchronous_metrics_update_period_s>1</asynchronous_metrics_update_period_s></yandex>" \
        > /etc/clickhouse-server/config.d/asynchronous_metrics_update_period_s.xml

    # Set maximum memory usage as half of total memory (less chance of OOM).
    echo "<yandex><max_server_memory_usage_to_ram_ratio>0.5</max_server_memory_usage_to_ram_ratio></yandex>" \
        > /etc/clickhouse-server/config.d/max_server_memory_usage_to_ram_ratio.xml
}

function stop()
{
    clickhouse stop
}

function start()
{
    # Rename existing log file - it will be more convenient to read separate files for separate server runs.
    if [ -f '/var/log/clickhouse-server/clickhouse-server.log' ]
    then
        log_file_counter=1
        while [ -f "/var/log/clickhouse-server/clickhouse-server.log.${log_file_counter}" ]
        do
            log_file_counter=$((log_file_counter + 1))
        done
        mv '/var/log/clickhouse-server/clickhouse-server.log' "/var/log/clickhouse-server/clickhouse-server.log.${log_file_counter}"
    fi

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
        # use root to match with current uid
        clickhouse start --user root >/var/log/clickhouse-server/stdout.log 2>/var/log/clickhouse-server/stderr.log
        sleep 0.5
        counter=$((counter + 1))
    done

    echo "
set follow-fork-mode child
handle all noprint
handle SIGSEGV stop print
handle SIGBUS stop print
handle SIGABRT stop print
continue
thread apply all backtrace
detach
quit
" > script.gdb

    # FIXME Hung check may work incorrectly because of attached gdb
    # 1. False positives are possible
    # 2. We cannot attach another gdb to get stacktraces if some queries hung
    gdb -batch -command script.gdb -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" >> /test_output/gdb.log &
}

configure

start

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
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

./stress --hung-check --drop-databases --output-folder test_output --skip-func-tests "$SKIP_TESTS_OPTION" \
    && echo -e 'Test script exit code\tOK' >> /test_output/test_results.tsv \
    || echo -e 'Test script failed\tFAIL' >> /test_output/test_results.tsv

stop
start

clickhouse-client --query "SELECT 'Server successfully started', 'OK'" >> /test_output/test_results.tsv \
                       || echo -e 'Server failed to start\tFAIL' >> /test_output/test_results.tsv

[ -f /var/log/clickhouse-server/clickhouse-server.log ] || echo -e "Server log does not exist\tFAIL"
[ -f /var/log/clickhouse-server/stderr.log ] || echo -e "Stderr log does not exist\tFAIL"

# Print Fatal log messages to stdout
zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.log

# Grep logs for sanitizer asserts, crashes and other critical errors

# Sanitizer asserts
zgrep -Fa "==================" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
zgrep -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
zgrep -Fav "ASan doesn't fully support makecontext/swapcontext functions" > /dev/null \
    && echo -e 'Sanitizer assert (in stderr.log)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No sanitizer asserts\tOK' >> /test_output/test_results.tsv
rm -f /test_output/tmp

# OOM
zgrep -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server.log > /dev/null \
    && echo -e 'OOM killer (or signal 9) in clickhouse-server.log\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No OOM messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

# Logical errors
zgrep -Fa "Code: 49, e.displayText() = DB::Exception:" /var/log/clickhouse-server/clickhouse-server.log > /dev/null \
    && echo -e 'Logical error thrown (see clickhouse-server.log)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No logical errors\tOK' >> /test_output/test_results.tsv

# Crash
zgrep -Fa "########################################" /var/log/clickhouse-server/clickhouse-server.log > /dev/null \
    && echo -e 'Killed by signal (in clickhouse-server.log)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'Not crashed\tOK' >> /test_output/test_results.tsv

# It also checks for crash without stacktrace (printed by watchdog)
zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.log > /dev/null \
    && echo -e 'Fatal message in clickhouse-server.log\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No fatal messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

zgrep -Fa "########################################" /test_output/* > /dev/null \
    && echo -e 'Killed by signal (output files)\tFAIL' >> /test_output/test_results.tsv

# Put logs into /test_output/
for log_file in /var/log/clickhouse-server/clickhouse-server.log*
do
    pigz < "${log_file}" > /test_output/"$(basename ${log_file})".gz
done

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:
mv /var/log/clickhouse-server/stderr.log /test_output/
tar -chf /test_output/query_log_dump.tar /var/lib/clickhouse/data/system/query_log ||:
tar -chf /test_output/trace_log_dump.tar /var/lib/clickhouse/data/system/trace_log ||:

# Write check result into check_status.tsv
clickhouse-local --structure "test String, res String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by (lower(test) like '%hung%') LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv
