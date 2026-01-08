#!/bin/bash
# shellcheck disable=SC2086,SC2001,SC2046,SC2030,SC2031,SC2010,SC2015

set -x

# core.COMM.PID-TID
sysctl kernel.core_pattern='core.%e.%p-%P'
dmesg --clear ||:

set -e
set -u
set -o pipefail

stage=${stage:-}

repo_dir=/repo

CONFIG_DIR="/etc/clickhouse-server"

export PATH="$repo_dir/ci/tmp/:$PATH"
export PYTHONPATH=$repo_dir:$repo_dir/ci

cd /workspace

function configure
{
    chmod +x $repo_dir/ci/tmp/clickhouse
    # clickhouse may be compressed - run once to decompress
    $repo_dir/ci/tmp/clickhouse --query "SELECT 1" ||:
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-server
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-client
    ln -sf $repo_dir/ci/tmp/clickhouse $repo_dir/ci/tmp/clickhouse-local

    (cd $repo_dir && python3 $repo_dir/ci/jobs/scripts/clickhouse_proc.py logs_export_config) || { echo "Failed to create log export config"; exit 1; }
}

function debug_server
{
    # Debug.
    date
    sleep 10
    jobs
    pstree -aspgT
}

function fuzz
{
    pip install -r "$repo_dir/ci/jobs/scripts/fuzzer/dolor/requirements.txt"

    # server.log -> All server logs
    # stderr.log -> Process logs only

    server_seed=$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')
    echo "Server seed to use: $server_seed"
    server_logfile="$PWD/dolor.log"

    server_cmd=(
        python3
        "$repo_dir"/tests/casa_del_dolor/dolor.py
        --generator=buzzhouse
        --server-config=$repo_dir/ci/jobs/scripts/fuzzer/dolor/config.xml
        --user-config=$repo_dir/ci/jobs/scripts/fuzzer/dolor/users.xml
        --client-binary=$repo_dir/ci/tmp/clickhouse-client
        --server-binaries=$repo_dir/ci/tmp/clickhouse-server
        --client-config=$PWD/fuzz.json
        --server-settings-prob=0
        --change-server-version-prob=100
        --replica-values=1
        --shard-values=1
        --log-path=$server_logfile
        --add-disk-settings-prob=80
        --number-disks=1,3
        --add-filesystem-caches-prob=80
        --number-caches=1,1
        --time-between-shutdowns=300,300
        --restart-clickhouse-prob=75
        --compare-table-dump-prob=0
        --seed=$server_seed
        --add-policy-settings-prob=80
        --kill-server-prob=50
        --time-between-integration-shutdowns=1,10
        --mem-limit='32gb'
        --timeout=30
        --time-between-monitoring-runs=15,30
        --keeper-settings-prob=0
        --set-locales-prob=0
        --set-timezones-prob=0
        --set-shared-mergetree-disk
    )
    (( RANDOM % 5 )) && cmd+=(--without-minio)
    (( RANDOM % 4 )) && cmd+=(--with-azurite)
    (( RANDOM % 4 )) && cmd+=(--with-postgresql)
    (( RANDOM % 4 )) && cmd+=(--with-mysql)
    (( RANDOM % 4 )) && cmd+=(--with-nginx)
    (( RANDOM % 4 )) && cmd+=(--with-sqlite)
    (( RANDOM % 4 )) && cmd+=(--with-mongodb)
    (( RANDOM % 4 )) && cmd+=(--with-redis)
    (( RANDOM % 5 )) && cmd+=(--with-arrowflight)
    (( RANDOM % 4 )) && cmd+=(--with-kafka)
    (( RANDOM % 4 )) && cmd+=(--with-spark)
    (( RANDOM % 4 )) && cmd+=(--with-glue)
    (( RANDOM % 4 )) && cmd+=(--with-rest)
    (( RANDOM % 4 )) && cmd+=(--with-hms)
    (( RANDOM % 4 )) && cmd+=(--without-monitoring)
    (( RANDOM % 5 )) && cmd+=(--without-zookeeper)

    echo "Starting server fuzzer with command: ${server_cmd[*]}"
    "${server_cmd[@]}" >> "fuzzer.log" 2>&1 &

    for _ in {1..12}
    do
        if clickhouse-client --query "select 1"
        then
            break
        fi
        sleep 5
    done

    echo 'Server started and responded.'

    (cd $repo_dir && python3 $repo_dir/ci/jobs/scripts/clickhouse_proc.py logs_export_start) || { echo "Failed to start log exports"; exit 1; }

    fuzzer_pid=$(sed -n 's/.*Load generator started with PID \([0-9]\+\).*/\1/p' $server_logfile)
    echo "Fuzzer pid is $fuzzer_pid"

    debug_server &
    server_exit_code=$(sed -n 's/.*The server node0 exited with code: \([0-9]\+\).*/\1/p' $server_logfile)
    echo "Server exit code is $server_exit_code"
    server_died=$(grep -Fq "The server node0 is not running" $server_logfile)
    echo "Server died value is $server_died"
    fuzzer_exit_code=$(sed -n 's/.*Load generator exited with code: \([0-9]\+\).*/\1/p' $server_logfile)
    echo "Fuzzer exit code is $fuzzer_exit_code"

    echo -e "$server_died\t$server_exit_code\t$fuzzer_exit_code" > status.tsv

    mv ./_instances-dolor.py/node0/logs/clickhouse-server.log server.log
    mv ./_instances-dolor.py/node0/logs/stderr.log stderr.log
    mv ./_instances-dolor.py/node0/logs/stdout.log stdout.log

    if test -f core.*; then
        zstd --threads=0 core.*
        mv core.*.zst core.zst
    fi
}

case "$stage" in
"")
    ;&  # Did you know? This is "fallthrough" in bash. https://stackoverflow.com/questions/12010686/case-statement-fallthrough
"configure")
    time configure
    ;&
"fuzz")
    time fuzz
    ;&
esac

exit $server_exit_code
