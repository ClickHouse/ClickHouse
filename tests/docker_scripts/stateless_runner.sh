#!/bin/bash

# fail on errors, verbose and export all env variables
set -e -x -a

# shellcheck disable=SC1091
source /setup_export_logs.sh

# shellcheck source=../stateless/stress_tests.lib
source /repo/tests/docker_scripts/stress_tests.lib

# Avoid overlaps with previous runs
dmesg --clear

# fail on errors, verbose and export all env variables
set -e -x -a

USE_DATABASE_REPLICATED=${USE_DATABASE_REPLICATED:=0}
USE_SHARED_CATALOG=${USE_SHARED_CATALOG:=0}
RUN_SEQUENTIAL_TESTS_IN_PARALLEL=1

if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]] || [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
  RUN_SEQUENTIAL_TESTS_IN_PARALLEL=0
fi
# Choose random timezone for this test run.
#
# NOTE: that clickhouse-test will randomize session_timezone by itself as well
# (it will choose between default server timezone and something specific).
TZ="$(rg -v '#' /usr/share/zoneinfo/zone.tab  | awk '{print $3}' | shuf | head -n1)"
echo "Chosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

dpkg -i package_folder/clickhouse-common-static_*.deb
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-odbc-bridge_*.deb
dpkg -i package_folder/clickhouse-library-bridge_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

echo "$BUGFIX_VALIDATE_CHECK"

# Check that the tools are available under short names
if [[ -z "$BUGFIX_VALIDATE_CHECK" ]]; then
    ch --query "SELECT 1" || exit 1
    chl --query "SELECT 1" || exit 1
    chc --version || exit 1
fi

ln -sf /repo/tests/clickhouse-test /usr/bin/clickhouse-test

export CLICKHOUSE_GRPC_CLIENT="/repo/utils/grpc-client/clickhouse-grpc-client.py"

# shellcheck disable=SC1091
source /repo/tests/docker_scripts/attach_gdb.lib

# shellcheck disable=SC1091
source /repo/tests/docker_scripts/utils.lib

# install test configs
/repo/tests/config/install.sh

/repo/tests/docker_scripts/setup_minio.sh stateless

/repo/tests/docker_scripts/setup_hdfs_minicluster.sh

config_logs_export_cluster /etc/clickhouse-server/config.d/system_logs_export.yaml

if [[ -n "$BUGFIX_VALIDATE_CHECK" ]] && [[ "$BUGFIX_VALIDATE_CHECK" -eq 1 ]]; then
    sudo sed -i "/<use_compression>1<\/use_compression>/d" /etc/clickhouse-server/config.d/zookeeper.xml

    # it contains some new settings, but we can safely remove it
    rm /etc/clickhouse-server/config.d/handlers.yaml
    rm /etc/clickhouse-server/users.d/s3_cache_new.xml
    rm /etc/clickhouse-server/config.d/zero_copy_destructive_operations.xml

    function remove_keeper_config()
    {
        sudo sed -i "/<$1>$2<\/$1>/d" /etc/clickhouse-server/config.d/keeper_port.xml
    }
    # commit_logs_cache_size_threshold setting doesn't exist on some older versions
    remove_keeper_config "commit_logs_cache_size_threshold" "[[:digit:]]\+"
    remove_keeper_config "latest_logs_cache_size_threshold" "[[:digit:]]\+"
fi

export IS_FLAKY_CHECK=0

# Export NUM_TRIES so python scripts will see its value as env variable
export NUM_TRIES

# For flaky check we also enable thread fuzzer
if [ "$NUM_TRIES" -gt "1" ]; then
    export IS_FLAKY_CHECK=1

    export THREAD_FUZZER_CPU_TIME_PERIOD_US=1000
    export THREAD_FUZZER_SLEEP_PROBABILITY=0.1
    export THREAD_FUZZER_SLEEP_TIME_US_MAX=100000

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY=1
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY=1

    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY=0.001
    export THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US_MAX=10000
    export THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US_MAX=10000
    export THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US_MAX=10000
    export THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US_MAX=10000

    mkdir -p /var/run/clickhouse-server
fi

# Run a CH instance to execute sequential tests on it in parallel with all other tests.
if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
  mkdir -p /var/run/clickhouse-server3 /etc/clickhouse-server3 /var/lib/clickhouse3
  # use half of available memory for each server
  sudo find /etc/clickhouse-server/ -type f -name '*.xml' -exec sed -i "s|<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>|<max_server_memory_usage_to_ram_ratio>0.46</max_server_memory_usage_to_ram_ratio>|g" {} \;
  cp -r -L /etc/clickhouse-server/* /etc/clickhouse-server3/

  sudo chown clickhouse:clickhouse /var/run/clickhouse-server3 /var/lib/clickhouse3 /etc/clickhouse-server3/
  sudo chown -R clickhouse:clickhouse /etc/clickhouse-server3/*

  function replace(){
      sudo find /etc/clickhouse-server3/ -type f -name '*.xml' -exec sed -i "$1" {} \;
  }

  replace "s|<port>9000</port>|<port>19000</port>|g"
  replace "s|<port>9440</port>|<port>19440</port>|g"
  replace "s|<port>9988</port>|<port>19988</port>|g"
  replace "s|<port>9234</port>|<port>19234</port>|g"
  replace "s|<port>9181</port>|<port>19181</port>|g"
  replace "s|<https_port>8443</https_port>|<https_port>18443</https_port>|g"
  replace "s|<tcp_port>9000</tcp_port>|<tcp_port>19000</tcp_port>|g"
  replace "s|<tcp_port>9181</tcp_port>|<tcp_port>19181</tcp_port>|g"
  replace "s|<tcp_port_secure>9440</tcp_port_secure>|<tcp_port_secure>19440</tcp_port_secure>|g"
  replace "s|<tcp_with_proxy_port>9010</tcp_with_proxy_port>|<tcp_with_proxy_port>19010</tcp_with_proxy_port>|g"
  replace "s|<mysql_port>9004</mysql_port>|<mysql_port>19004</mysql_port>|g"
  replace "s|<postgresql_port>9005</postgresql_port>|<postgresql_port>19005</postgresql_port>|g"
  replace "s|<interserver_http_port>9009</interserver_http_port>|<interserver_http_port>19009</interserver_http_port>|g"
  replace "s|8123|18123|g"
  replace "s|/var/lib/clickhouse/|/var/lib/clickhouse3/|g"
  replace "s|/etc/clickhouse-server/|/etc/clickhouse-server3/|g"
  # distributed cache
  replace "s|<tcp_port>10001</tcp_port>|<tcp_port>10004</tcp_port>|g"
  replace "s|<tcp_port>10002</tcp_port>|<tcp_port>10005</tcp_port>|g"
  replace "s|<tcp_port>10003</tcp_port>|<tcp_port>10006</tcp_port>|g"
#  replace "s|<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>|<max_server_memory_usage_to_ram_ratio>0.46</max_server_memory_usage_to_ram_ratio>|g"

  sudo -E -u clickhouse /usr/bin/clickhouse server --daemon --config /etc/clickhouse-server3/config.xml \
  --pid-file /var/run/clickhouse-server3/clickhouse-server.pid \
  -- --path /var/lib/clickhouse3/ --logger.stderr /var/log/clickhouse-server/stderr-no-parallel.log \
  --logger.log /var/log/clickhouse-server/clickhouse-server-no-parallel.log \
  --logger.errorlog /var/log/clickhouse-server/clickhouse-server-no-parallel.err.log \
  --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
  --prometheus.port 19988 --keeper_server.raft_configuration.server.port 19234 --keeper_server.tcp_port 19181 \
  --mysql_port 19004 --postgresql_port 19005

  for _ in {1..100}
  do
      clickhouse-client --port 19000 --query "SELECT 1" && break
      sleep 1
  done

fi
# simplest way to forward env variables to server
sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon --pid-file /var/run/clickhouse-server/clickhouse-server.pid

if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo sed -i "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_1/</filesystem_caches_path>|" /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

    sudo sed -i "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_2/</filesystem_caches_path>|" /etc/clickhouse-server2/config.d/filesystem_caches_path.xml

    sudo sed -i "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_1/</custom_cached_disks_base_directory>|" /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

    sudo sed -i "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_2/</custom_cached_disks_base_directory>|" /etc/clickhouse-server2/config.d/filesystem_caches_path.xml

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
fi

if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
    sudo cat /etc/clickhouse-server1/config.d/filesystem_caches_path.xml \
    | sed "s|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>|<filesystem_caches_path>/var/lib/clickhouse/filesystem_caches_1/</filesystem_caches_path>|" \
    > /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

    sudo cat /etc/clickhouse-server1/config.d/filesystem_caches_path.xml \
    | sed "s|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches/</custom_cached_disks_base_directory>|<custom_cached_disks_base_directory replace=\"replace\">/var/lib/clickhouse/filesystem_caches_1/</custom_cached_disks_base_directory>|" \
    > /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp
    mv /etc/clickhouse-server1/config.d/filesystem_caches_path.xml.tmp /etc/clickhouse-server1/config.d/filesystem_caches_path.xml

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
fi

# Wait for the server to start, but not for too long.
for _ in {1..100}
do
    clickhouse-client --query "SELECT 1" && break
    sleep 1
done

setup_logs_replication
if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
  setup_logs_replication 19000
fi

attach_gdb_to_clickhouse

# create tables for minio log webhooks
clickhouse-client --query "CREATE TABLE minio_audit_logs
(
    log String,
    event_time DateTime64(9) MATERIALIZED parseDateTime64BestEffortOrZero(trim(BOTH '\"' FROM JSONExtractRaw(log, 'time')), 9, 'UTC')
)
ENGINE = MergeTree
ORDER BY tuple()"

clickhouse-client --query "CREATE TABLE minio_server_logs
(
    log String,
    event_time DateTime64(9) MATERIALIZED parseDateTime64BestEffortOrZero(trim(BOTH '\"' FROM JSONExtractRaw(log, 'time')), 9, 'UTC')
)
ENGINE = MergeTree
ORDER BY tuple()"

# create minio log webhooks for both audit and server logs
# use async inserts to avoid creating too many parts
./mc admin config set clickminio logger_webhook:ch_server_webhook endpoint="http://localhost:8123/?async_insert=1&wait_for_async_insert=0&async_insert_busy_timeout_min_ms=5000&async_insert_busy_timeout_max_ms=5000&async_insert_max_query_number=1000&async_insert_max_data_size=10485760&query=INSERT%20INTO%20minio_server_logs%20FORMAT%20LineAsString" queue_size=1000000 batch_size=500
./mc admin config set clickminio audit_webhook:ch_audit_webhook endpoint="http://localhost:8123/?async_insert=1&wait_for_async_insert=0&async_insert_busy_timeout_min_ms=5000&async_insert_busy_timeout_max_ms=5000&async_insert_max_query_number=1000&async_insert_max_data_size=10485760&query=INSERT%20INTO%20minio_audit_logs%20FORMAT%20LineAsString" queue_size=1000000 batch_size=500

max_retries=100
retry=1
while [ $retry -le $max_retries ]; do
    echo "clickminio restart attempt $retry:"

    output=$(./mc admin service restart clickminio --wait --json 2>&1 | jq -r .status)
    echo "Output of restart status: $output"

    expected_output="success
success"
    if [ "$output" = "$expected_output" ]; then
        echo "Restarted clickminio successfully."
        break
    fi

    sleep 1

    retry=$((retry + 1))
done

if [ $retry -gt $max_retries ]; then
    echo "Failed to restart clickminio after $max_retries attempts."
fi

./mc admin trace clickminio > /test_output/minio.log &
MC_ADMIN_PID=$!

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

    if [[ -n "$USE_AZURE_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_AZURE_STORAGE_FOR_MERGE_TREE"  -eq 1 ]]; then
        # to disable the same tests
        ADDITIONAL_OPTIONS+=('--azure-blob-storage')
        # azurite is slow, but with these two settings it can be super slow
        ADDITIONAL_OPTIONS+=('--no-random-settings')
        ADDITIONAL_OPTIONS+=('--no-random-merge-tree-settings')
    fi

    if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--shared-catalog')
    fi

    if [[ "$USE_DISTRIBUTED_CACHE" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--distributed-cache')
    fi

    if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
        # Too many tests fail for DatabaseReplicated in parallel.
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('3')
    elif [[ 1 == $(clickhouse-client --query "SELECT value LIKE '%SANITIZE_COVERAGE%' FROM system.build_options WHERE name = 'CXX_FLAGS'") ]]; then
        # Coverage on a per-test basis could only be collected sequentially.
        # Do not set the --jobs parameter.
        echo "Running tests with coverage collection."
    else
        # All other configurations are OK.
        ADDITIONAL_OPTIONS+=('--jobs')
        ADDITIONAL_OPTIONS+=('3')
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

    TEST_ARGS=(
        --testname
        --shard
        --zookeeper
        --check-zookeeper-session
        --hung-check
        --print-time
        --no-drop-if-fail
        --capture-client-stacktrace
        --queries "/repo/tests/queries"
        --test-runs "$NUM_TRIES"
        "${ADDITIONAL_OPTIONS[@]}"
    )
    clickhouse-test "${TEST_ARGS[@]}" 2>&1 \
        | ts '%Y-%m-%d %H:%M:%S' \
        | tee -a test_output/test_result.txt
    set -e
}

export -f run_tests

if [ "$NUM_TRIES" -gt "1" ]; then
    # We don't run tests with Ordinary database in PRs, only in master.
    # So run new/changed tests with Ordinary at least once in flaky check.
    NUM_TRIES=1 USE_DATABASE_ORDINARY=1 run_tests \
      | sed 's/All tests have finished/Redacted: a message about tests finish is deleted/' | sed 's/No tests were run/Redacted: a message about no tests run is deleted/' ||:
fi

function run_no_parallel_test()
{
    export CLICKHOUSE_CLIENT_OPT=" --port 19000 "
    export CLICKHOUSE_CONFIG="/etc/clickhouse-server3/config.xml"
    export CLICKHOUSE_CONFIG_DIR="/etc/clickhouse-server3"
    export CLICKHOUSE_CONFIG_GREP="/etc/clickhouse-server3/preprocessed/config.xml"
    export CLICKHOUSE_USER_FILES="/var/lib/clickhouse3/user_files"
    export CLICKHOUSE_SCHEMA_FILES="/var/lib/clickhouse3/format_schemas"
    export CLICKHOUSE_PATH="/var/lib/clickhouse3"
    export CLICKHOUSE_PORT_TCP="19000"
    export CLICKHOUSE_PORT_TCP_SECURE="19440"
    export CLICKHOUSE_PORT_TCP_WITH_PROXY="19010"
    export CLICKHOUSE_PORT_HTTP="18123"
    export CLICKHOUSE_PORT_HTTPS="18443"
    export CLICKHOUSE_PORT_INTERSERVER="19009"
    export CLICKHOUSE_PORT_KEEPER="19181"
    export CLICKHOUSE_PORT_PROMTHEUS_PORT="19988"
    export CLICKHOUSE_PORT_MYSQL="19004"
    export CLICKHOUSE_PORT_POSTGRESQL="19005"
    export CLICKHOUSE_WRITE_COVERAGE="coverage_no_parallel"
    export ADDITIONAL_OPTIONS=("--run-no-parallel-only")
    run_tests
}


if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
  run_no_parallel_test &
  PID1=$!

  export ADDITIONAL_OPTIONS=("--run-parallel-only")
  run_tests &
  PID2=$!

  wait $PID1 $PID2 ||:
else
  run_tests ||:
fi


echo "Files in current directory"
ls -la ./
echo "Files in root directory"
ls -la /

/repo/tests/docker_scripts/process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

clickhouse-client -q "system flush logs" ||:

# stop logs replication to make it possible to dump logs tables via clickhouse-local
stop_logs_replication
if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
  stop_logs_replication 19000
fi

# Try to get logs while server is running
failed_to_save_logs=0
for table in query_log zookeeper_log trace_log transactions_info_log metric_log blob_storage_log error_log
do
    if ! clickhouse-client -q "select * from system.$table into outfile '/test_output/$table.tsv.zst' format TSVWithNamesAndTypes"; then
        failed_to_save_logs=1
    fi

    if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
        if ! clickhouse-client --port 19000 -q "select * from system.$table into outfile '/test_output/$table.3.tsv.zst' format TSVWithNamesAndTypes"; then
            failed_to_save_logs=1
        fi
    fi
    if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        if ! clickhouse-client --port 19000 -q "select * from system.$table into outfile '/test_output/$table.1.tsv.zst' format TSVWithNamesAndTypes"; then
            failed_to_save_logs=1
        fi
        if ! clickhouse-client --port 29000 -q "select * from system.$table into outfile '/test_output/$table.2.tsv.zst' format TSVWithNamesAndTypes"; then
            failed_to_save_logs=1
        fi
    fi

    if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
        if ! clickhouse-client --port 29000 -q "select * from system.$table into outfile '/test_output/$table.2.tsv.zst' format TSVWithNamesAndTypes"; then
            failed_to_save_logs=1
        fi
    fi
done


# collect minio audit and server logs
# wait for minio to flush its batch if it has any
sleep 1
clickhouse-client -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
clickhouse-client --max_block_size 8192 --max_memory_usage 10G --max_threads 1 --max_result_bytes 0 --max_result_rows 0 --max_rows_to_read 0 --max_bytes_to_read 0 -q "SELECT log FROM minio_audit_logs ORDER BY event_time INTO OUTFILE '/test_output/minio_audit_logs.jsonl.zst' FORMAT JSONEachRow"
clickhouse-client --max_block_size 8192 --max_memory_usage 10G --max_threads 1 --max_result_bytes 0 --max_result_rows 0 --max_rows_to_read 0 --max_bytes_to_read 0 -q "SELECT log FROM minio_server_logs ORDER BY event_time INTO OUTFILE '/test_output/minio_server_logs.jsonl.zst' FORMAT JSONEachRow"

# Stop server so we can safely read data with clickhouse-local.
# Why do we read data with clickhouse-local?
# Because it's the simplest way to read it when server has crashed.
sudo clickhouse stop ||:

if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server3 ||:
fi

if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server1 ||:
    sudo clickhouse stop --pid-path /var/run/clickhouse-server2 ||:
fi

if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server1 ||:
fi

# Kill minio admin client to stop collecting logs
kill $MC_ADMIN_PID

rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server.log ||:
rg -A50 -Fa "============" /var/log/clickhouse-server/stderr.log ||:
zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.zst &

data_path_config="--path=/var/lib/clickhouse/"
if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
    # We need s3 storage configuration (but it's more likely that clickhouse-local will fail for some reason)
    data_path_config="--config-file=/etc/clickhouse-server/config.xml"
fi


# If server crashed dump system logs with clickhouse-local
if [ $failed_to_save_logs -ne 0 ]; then
    # Compress tables.
    #
    # NOTE:
    # - that due to tests with s3 storage we cannot use /var/lib/clickhouse/data
    #   directly
    # - even though ci auto-compress some files (but not *.tsv) it does this only
    #   for files >64MB, we want this files to be compressed explicitly
    for table in query_log zookeeper_log trace_log transactions_info_log metric_log blob_storage_log error_log
    do
        clickhouse-local "$data_path_config" --only-system-tables --stacktrace -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.tsv.zst ||:

        if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
            clickhouse-local --path /var/lib/clickhouse3/ --only-system-tables --stacktrace -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.3.tsv.zst ||:
        fi

        if
         [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
            clickhouse-local --path /var/lib/clickhouse1/ --only-system-tables --stacktrace -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.1.tsv.zst ||:
            clickhouse-local --path /var/lib/clickhouse2/ --only-system-tables --stacktrace -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.2.tsv.zst ||:
        fi

        if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
            clickhouse-local --path /var/lib/clickhouse1/ --only-system-tables --stacktrace -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.1.tsv.zst ||:
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

# Grep logs for sanitizer asserts, crashes and other critical errors
check_logs_for_critical_errors

# Compressed (FIXME: remove once only github actions will be left)
rm /var/log/clickhouse-server/clickhouse-server.log
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:
if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar --zstd -chf /test_output/clickhouse_coverage.tar.zst /profraw ||:
fi

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

rm -rf /var/lib/clickhouse/data/system/*/
tar -chf /test_output/store.tar /var/lib/clickhouse/store ||:
tar -chf /test_output/metadata.tar /var/lib/clickhouse/metadata/*.sql ||:

if [[ "$RUN_SEQUENTIAL_TESTS_IN_PARALLEL" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server-no-parallel.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server-no-parallel.log > /test_output/clickhouse-server-no-parallel.log.zst ||:
    mv /var/log/clickhouse-server/stderr-no-parallel.log /test_output/ ||:
    tar -chf /test_output/coordination-no-parallel.tar /var/lib/clickhouse3/coordination ||:
fi

if [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server1.log ||:
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server2.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.zst ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.zst ||:
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
    tar -chf /test_output/coordination1.tar /var/lib/clickhouse1/coordination ||:
    tar -chf /test_output/coordination2.tar /var/lib/clickhouse2/coordination ||:
fi

if [[ "$USE_SHARED_CATALOG" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server1.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.zst ||:
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    tar -chf /test_output/coordination1.tar /var/lib/clickhouse1/coordination ||:
fi

collect_core_dumps
