#!/bin/bash

# shellcheck disable=SC1091
source /setup_export_logs.sh

# fail on errors, verbose and export all env variables
set -e -x -a

# Choose random timezone for this test run.
#
# NOTE: that clickhouse-test will randomize session_timezone by itself as well
# (it will choose between default server timezone and something specific).
TZ="$(rg -v '#' /usr/share/zoneinfo/zone.tab  | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

echo "$BUGFIX_VALIDATE_CHECK"

# Check that the tools are available under short names
if [[ -z "$BUGFIX_VALIDATE_CHECK" ]]; then
    ch --query "SELECT 1" || exit 1
    chl --query "SELECT 1" || exit 1
    chc --version || exit 1
fi

ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test

# shellcheck disable=SC1091
source /attach_gdb.lib

# shellcheck disable=SC1091
source /utils.lib

# install test configs
/usr/share/clickhouse-test/config/install.sh

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    echo "Azure is disabled"
else
    azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --debug /azurite_log &
fi

./setup_minio.sh stateless
./setup_hdfs_minicluster.sh

config_logs_export_cluster /etc/clickhouse-server/config.d/system_logs_export.yaml

if [[ -n "$BUGFIX_VALIDATE_CHECK" ]] && [[ "$BUGFIX_VALIDATE_CHECK" -eq 1 ]]; then
    sudo cat /etc/clickhouse-server/config.d/zookeeper.xml \
    | sed "/<use_compression>1<\/use_compression>/d" \
    > /etc/clickhouse-server/config.d/zookeeper.xml.tmp
    sudo mv /etc/clickhouse-server/config.d/zookeeper.xml.tmp /etc/clickhouse-server/config.d/zookeeper.xml

    # it contains some new settings, but we can safely remove it
    rm /etc/clickhouse-server/users.d/s3_cache_new.xml
    rm /etc/clickhouse-server/config.d/zero_copy_destructive_operations.xml
fi

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

    mkdir -p /var/run/clickhouse-server
    # simplest way to forward env variables to server
    sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon --pid-file /var/run/clickhouse-server/clickhouse-server.pid
else
    sudo clickhouse start
fi

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo cat /etc/clickhouse-server1/config.d/filesystem_caches_path.xml \
    | sed "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_1/</filesystem_caches_path>|" \
    > /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

    sudo cat /etc/clickhouse-server2/config.d/filesystem_caches_path.xml \
    | sed "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_2/</filesystem_caches_path>|" \
    > /etc/clickhouse-server2/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server2/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server2/config.d/filesystem_caches_path.xml

    sudo cat /etc/clickhouse-server1/config.d/filesystem_caches_path.xml \
    | sed "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_1/</custom_cached_disks_base_directory>|" \
    > /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

    sudo cat /etc/clickhouse-server2/config.d/filesystem_caches_path.xml \
    | sed "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_2/</custom_cached_disks_base_directory>|" \
    > /etc/clickhouse-server2/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server2/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server2/config.d/filesystem_caches_path.xml

    mkdir -p /var/run/clickhouse-server1
    sudo chown clickhouse:clickhouse /var/run/clickhouse-server1
    sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server1/config.xml --daemon \
    --pid-file /var/run/clickhouse-server1/clickhouse-server.pid \
    -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
    --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
    --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
    --mysql_port 19004 --postgresql_port 19005 \
    --keeper_server.tcp_port 19181 --keeper_server.server_id 2 \
    --prometheus.port 19988 \
    --macros.replica r2   # It doesn't work :(

    mkdir -p /var/run/clickhouse-server2
    sudo chown clickhouse:clickhouse /var/run/clickhouse-server2
    sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
    --pid-file /var/run/clickhouse-server2/clickhouse-server.pid \
    -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
    --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
    --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \
    --mysql_port 29004 --postgresql_port 29005 \
    --keeper_server.tcp_port 29181 --keeper_server.server_id 3 \
    --prometheus.port 29988 \
    --macros.shard s2   # It doesn't work :(

    MAX_RUN_TIME=$((MAX_RUN_TIME < 9000 ? MAX_RUN_TIME : 9000))  # min(MAX_RUN_TIME, 2.5 hours)
    MAX_RUN_TIME=$((MAX_RUN_TIME != 0 ? MAX_RUN_TIME : 9000))    # set to 2.5 hours if 0 (unlimited)
fi


# Wait for the server to start, but not for too long.
for _ in {1..100}
do
    clickhouse-client --query "SELECT 1" && break
    sleep 1
done

setup_logs_replication

attach_gdb_to_clickhouse || true  # FIXME: to not break old builds, clean on 2023-09-01

function fn_exists() {
    declare -F "$1" > /dev/null;
}

# FIXME: to not break old builds, clean on 2023-09-01
function try_run_with_retry() {
    local total_retries="$1"
    shift

    if fn_exists run_with_retry; then
        run_with_retry "$total_retries" "$@"
    else
        "$@"
    fi
}

function run_tests()
{
    set -x
    # We can have several additional options so we pass them as array because it is more ideologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

    HIGH_LEVEL_COVERAGE=YES

    # Use random order in flaky check
    if [ "$NUM_TRIES" -gt "1" ]; then
        ADDITIONAL_OPTIONS+=('--order=random')
        HIGH_LEVEL_COVERAGE=NO
    fi

    if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--s3-storage')
    fi

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('2')
    else
        # Too many tests fail for DatabaseReplicated in parallel. All other
        # configurations are OK.
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('8')
    fi

    if [[ -n "$RUN_BY_HASH_NUM" ]] && [[ -n "$RUN_BY_HASH_TOTAL" ]]; then
        ADDITIONAL_OPTIONS+=('--run-by-hash-num')
        ADDITIONAL_OPTIONS+=("$RUN_BY_HASH_NUM")
        ADDITIONAL_OPTIONS+=('--run-by-hash-total')
        ADDITIONAL_OPTIONS+=("$RUN_BY_HASH_TOTAL")
        HIGH_LEVEL_COVERAGE=NO
    fi

    if [[ -n "$USE_DATABASE_ORDINARY" ]] && [[ "$USE_DATABASE_ORDINARY" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--db-engine=Ordinary')
    fi

    if [[ "${HIGH_LEVEL_COVERAGE}" = "YES" ]]; then
        ADDITIONAL_OPTIONS+=('--report-coverage')
    fi

    ADDITIONAL_OPTIONS+=('--report-logs-stats')

    try_run_with_retry 10 clickhouse-client -q "insert into system.zookeeper (name, path, value) values ('auxiliary_zookeeper2', '/test/chroot/', '')"

    set +e
    clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check --print-time \
        --test-runs "$NUM_TRIES" "${ADDITIONAL_OPTIONS[@]}" 2>&1 \
    | ts '%Y-%m-%d %H:%M:%S' \
    | tee -a test_output/test_result.txt
    set -e
}

export -f run_tests

if [ "$NUM_TRIES" -gt "1" ]; then
    # We don't run tests with Ordinary database in PRs, only in master.
    # So run new/changed tests with Ordinary at least once in flaky check.
    timeout_with_logging "$MAX_RUN_TIME" bash -c 'NUM_TRIES=1; USE_DATABASE_ORDINARY=1; run_tests' \
      | sed 's/All tests have finished//' | sed 's/No tests were run//' ||:
fi

timeout_with_logging "$MAX_RUN_TIME" bash -c run_tests ||:

echo "Files in current directory"
ls -la ./
echo "Files in root directory"
ls -la /

/process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

clickhouse-client -q "system flush logs" ||:

# stop logs replication to make it possible to dump logs tables via clickhouse-local
stop_logs_replication

# Try to get logs while server is running
successfuly_saved=0
for table in query_log zookeeper_log trace_log transactions_info_log metric_log
do
    clickhouse-client -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.tsv.zst
    successfuly_saved=$?
    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        clickhouse-client -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.1.tsv.zst
        successfuly_saved=$((successfuly_saved | $?))
        clickhouse-client -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.2.tsv.zst
        successfuly_saved=$((successfuly_saved | $?))
    fi
done

# Stop server so we can safely read data with clickhouse-local.
# Why do we read data with clickhouse-local?
# Because it's the simplest way to read it when server has crashed.
sudo clickhouse stop ||:
if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server1 ||:
    sudo clickhouse stop --pid-path /var/run/clickhouse-server2 ||:
fi

rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server.log ||:
rg -A50 -Fa "============" /var/log/clickhouse-server/stderr.log ||:
zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.zst &

data_path_config="--path=/var/lib/clickhouse/"
if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
    # We need s3 storage configuration (but it's more likely that clickhouse-local will fail for some reason)
    data_path_config="--config-file=/etc/clickhouse-server/config.xml"
fi


# If server crashed dump system logs with clickhouse-local
if [ $successfuly_saved -ne 0 ]; then
    # Compress tables.
    #
    # NOTE:
    # - that due to tests with s3 storage we cannot use /var/lib/clickhouse/data
    #   directly
    # - even though ci auto-compress some files (but not *.tsv) it does this only
    #   for files >64MB, we want this files to be compressed explicitly
    for table in query_log zookeeper_log trace_log transactions_info_log metric_log
    do
        clickhouse-local "$data_path_config" --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.tsv.zst ||:
        if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
            clickhouse-local --path /var/lib/clickhouse1/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.1.tsv.zst ||:
            clickhouse-local --path /var/lib/clickhouse2/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.2.tsv.zst ||:
        fi
    done
fi

# Also export trace log in flamegraph-friendly format.
for trace_type in CPU Memory Real
do
    clickhouse-local "$data_path_config" --only-system-tables -q "
            select
                arrayStringConcat((arrayMap(x -> concat(splitByChar('/', addressToLine(x))[-1], '#', demangle(addressToSymbol(x)) ), trace)), ';') AS stack,
                count(*) AS samples
            from system.trace_log
            where trace_type = '$trace_type'
            group by trace
            order by samples desc
            settings allow_introspection_functions = 1
            format TabSeparated" \
        | zstd --threads=0 > "/test_output/trace-log-$trace_type-flamegraph.tsv.zst" ||:
done


# Compressed (FIXME: remove once only github actions will be left)
rm /var/log/clickhouse-server/clickhouse-server.log
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:
if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar --zstd -chf /test_output/clickhouse_coverage.tar.zst /profraw ||:
fi

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server1.log ||:
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server2.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.zst ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.zst ||:
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
    tar -chf /test_output/coordination1.tar /var/lib/clickhouse1/coordination ||:
    tar -chf /test_output/coordination2.tar /var/lib/clickhouse2/coordination ||:
fi
