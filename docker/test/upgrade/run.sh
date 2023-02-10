#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

# This script is similar to script for common stress test

# Avoid overlaps with previous runs
dmesg --clear

set -x

# core.COMM.PID-TID
sysctl kernel.core_pattern='core.%e.%p-%P'

OK="\tOK\t\\N\t"
FAIL="\tFAIL\t\\N\t"

FAILURE_CONTEXT_LINES=50
FAILURE_CONTEXT_MAX_LINE_WIDTH=400

function escaped()
{
    # That's the simplest way I found to escape a string in bash. Yep, bash is the most convenient programming language.
    # Also limit lines width just in case (too long lines are not really useful usually)
    clickhouse local -S 's String' --input-format=LineAsString -q "select substr(s, 1, $FAILURE_CONTEXT_MAX_LINE_WIDTH)
      from table format CustomSeparated settings format_custom_row_after_delimiter='\\\\\\\\n'"
}
function head_escaped()
{
    head -n $FAILURE_CONTEXT_LINES $1 | escaped
}
function unts()
{
    grep -Po "[0-9][0-9]:[0-9][0-9] \K.*"
}
function trim_server_logs()
{
    head -n $FAILURE_CONTEXT_LINES "/test_output/$1" | grep -Eo " \[ [0-9]+ \] \{.*" | escaped
}

function install_packages()
{
    dpkg -i $1/clickhouse-common-static_*.deb
    dpkg -i $1/clickhouse-common-static-dbg_*.deb
    dpkg -i $1/clickhouse-server_*.deb
    dpkg -i $1/clickhouse-client_*.deb
}

function configure()
{
    # install test configs
    export USE_DATABASE_ORDINARY=1
    export EXPORT_S3_STORAGE_POLICIES=1
    /usr/share/clickhouse-test/config/install.sh

    # avoid too slow startup
    sudo cat /etc/clickhouse-server/config.d/keeper_port.xml \
      | sed "s|<snapshot_distance>100000</snapshot_distance>|<snapshot_distance>10000</snapshot_distance>|" \
      > /etc/clickhouse-server/config.d/keeper_port.xml.tmp
    sudo mv /etc/clickhouse-server/config.d/keeper_port.xml.tmp /etc/clickhouse-server/config.d/keeper_port.xml
    sudo chown clickhouse /etc/clickhouse-server/config.d/keeper_port.xml
    sudo chgrp clickhouse /etc/clickhouse-server/config.d/keeper_port.xml

    # for clickhouse-server (via service)
    echo "ASAN_OPTIONS='malloc_context_size=10 verbosity=1 allocator_release_to_os_interval_ms=10000'" >> /etc/environment
    # for clickhouse-client
    export ASAN_OPTIONS='malloc_context_size=10 allocator_release_to_os_interval_ms=10000'

    # since we run clickhouse from root
    sudo chown root: /var/lib/clickhouse

    # Set more frequent update period of asynchronous metrics to more frequently update information about real memory usage (less chance of OOM).
    echo "<clickhouse><asynchronous_metrics_update_period_s>1</asynchronous_metrics_update_period_s></clickhouse>" \
        > /etc/clickhouse-server/config.d/asynchronous_metrics_update_period_s.xml


    local total_mem
    total_mem=$(awk '/MemTotal/ { print $(NF-1) }' /proc/meminfo) # KiB
    total_mem=$(( total_mem*1024 )) # bytes

    # Set maximum memory usage as half of total memory (less chance of OOM).
    #
    # But not via max_server_memory_usage but via max_memory_usage_for_user,
    # so that we can override this setting and execute service queries, like:
    # - hung check
    # - show/drop database
    # - ...
    #
    # So max_memory_usage_for_user will be a soft limit, and
    # max_server_memory_usage will be hard limit, and queries that should be
    # executed regardless memory limits will use max_memory_usage_for_user=0,
    # instead of relying on max_untracked_memory

    max_server_memory_usage_to_ram_ratio=0.5
    echo "Setting max_server_memory_usage_to_ram_ratio to ${max_server_memory_usage_to_ram_ratio}"
    cat > /etc/clickhouse-server/config.d/max_server_memory_usage.xml <<EOL
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>${max_server_memory_usage_to_ram_ratio}</max_server_memory_usage_to_ram_ratio>
</clickhouse>
EOL

    local max_users_mem
    max_users_mem=$((total_mem*30/100)) # 30%
    echo "Setting max_memory_usage_for_user=$max_users_mem and max_memory_usage for queries to 10G"
    cat > /etc/clickhouse-server/users.d/max_memory_usage_for_user.xml <<EOL
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>10G</max_memory_usage>
            <max_memory_usage_for_user>${max_users_mem}</max_memory_usage_for_user>
        </default>
    </profiles>
</clickhouse>
EOL

    cat > /etc/clickhouse-server/config.d/core.xml <<EOL
<clickhouse>
    <core_dump>
        <!-- 100GiB -->
        <size_limit>107374182400</size_limit>
    </core_dump>
    <!-- NOTE: no need to configure core_path,
         since clickhouse is not started as daemon (via clickhouse start)
    -->
    <core_path>$PWD</core_path>
</clickhouse>
EOL

    # Let OOM killer terminate other processes before clickhouse-server:
    cat > /etc/clickhouse-server/config.d/oom_score.xml <<EOL
<clickhouse>
    <oom_score>-1000</oom_score>
</clickhouse>
EOL

    # Analyzer is not yet ready for testing
    cat > /etc/clickhouse-server/users.d/no_analyzer.xml <<EOL
<clickhouse>
    <profiles>
        <default>
            <constraints>
                <allow_experimental_analyzer>
                    <readonly/>
                </allow_experimental_analyzer>
            </constraints>
        </default>
    </profiles>
</clickhouse>
EOL

}

function stop()
{
    local max_tries="${1:-90}"
    local pid
    # Preserve the pid, since the server can hung after the PID will be deleted.
    pid="$(cat /var/run/clickhouse-server/clickhouse-server.pid)"

    clickhouse stop --max-tries "$max_tries" --do-not-kill && return

    # We failed to stop the server with SIGTERM. Maybe it hang, let's collect stacktraces.
    echo -e "Possible deadlock on shutdown (see gdb.log)$FAIL" >> /test_output/test_results.tsv
    kill -TERM "$(pidof gdb)" ||:
    sleep 5
    echo "thread apply all backtrace (on stop)" >> /test_output/gdb.log
    timeout 30m gdb -batch -ex 'thread apply all backtrace' -p "$pid" | ts '%Y-%m-%d %H:%M:%S' >> /test_output/gdb.log
    clickhouse stop --force
}

function start()
{
    counter=0
    until clickhouse-client --query "SELECT 1"
    do
        if [ "$counter" -gt ${1:-120} ]
        then
            echo "Cannot start clickhouse-server"
            rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt ||:
            echo -e "Cannot start clickhouse-server$FAIL$(trim_server_logs application_errors.txt)" >> /test_output/test_results.tsv
            cat /var/log/clickhouse-server/stdout.log
            tail -n100 /var/log/clickhouse-server/stderr.log
            tail -n100000 /var/log/clickhouse-server/clickhouse-server.log | rg -F -v -e '<Warning> RaftInstance:' -e '<Information> RaftInstance' | tail -n100
            break
        fi
        # use root to match with current uid
        clickhouse start --user root >/var/log/clickhouse-server/stdout.log 2>>/var/log/clickhouse-server/stderr.log
        sleep 0.5
        counter=$((counter + 1))
    done

    # Set follow-fork-mode to parent, because we attach to clickhouse-server, not to watchdog
    # and clickhouse-server can do fork-exec, for example, to run some bridge.
    # Do not set nostop noprint for all signals, because some it may cause gdb to hang,
    # explicitly ignore non-fatal signals that are used by server.
    # Number of SIGRTMIN can be determined only in runtime.
    RTMIN=$(kill -l SIGRTMIN)
    echo "
set follow-fork-mode parent
handle SIGHUP nostop noprint pass
handle SIGINT nostop noprint pass
handle SIGQUIT nostop noprint pass
handle SIGPIPE nostop noprint pass
handle SIGTERM nostop noprint pass
handle SIGUSR1 nostop noprint pass
handle SIGUSR2 nostop noprint pass
handle SIG$RTMIN nostop noprint pass
info signals
continue
backtrace full
thread apply all backtrace full
info registers
disassemble /s
up
disassemble /s
up
disassemble /s
p \"done\"
detach
quit
" > script.gdb

    # FIXME Hung check may work incorrectly because of attached gdb
    # 1. False positives are possible
    # 2. We cannot attach another gdb to get stacktraces if some queries hung
    gdb -batch -command script.gdb -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" | ts '%Y-%m-%d %H:%M:%S' >> /test_output/gdb.log &
    sleep 5
    # gdb will send SIGSTOP, spend some time loading debug info and then send SIGCONT, wait for it (up to send_timeout, 300s)
    time clickhouse-client --query "SELECT 'Connected to clickhouse-server after attaching gdb'" ||:
}

# Thread Fuzzer allows to check more permutations of possible thread scheduling
# and find more potential issues.
# Temporarily disable ThreadFuzzer with tsan because of https://github.com/google/sanitizers/issues/1540
is_tsan_build=$(clickhouse local -q "select value like '% -fsanitize=thread %' from system.build_options where name='CXX_FLAGS'")
if [ "$is_tsan_build" -eq "0" ]; then
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
fi

azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --debug /azurite_log &
./setup_minio.sh stateless # to have a proper environment

# we mount tests folder from repo to /usr/share
ln -s /usr/share/clickhouse-test/ci/stress.py /usr/bin/stress
ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test
ln -s /usr/share/clickhouse-test/ci/download_release_packages.py /usr/bin/download_release_packages
ln -s /usr/share/clickhouse-test/ci/get_previous_release_tag.py /usr/bin/get_previous_release_tag

echo "Get previous release tag"
previous_release_tag=$(dpkg --info package_folder/clickhouse-client*.deb | grep "Version: " | awk '{print $2}' | cut -f1 -d'+' | get_previous_release_tag)
echo $previous_release_tag

echo "Clone previous release repository"
git clone https://github.com/ClickHouse/ClickHouse.git --no-tags --progress --branch=$previous_release_tag --no-recurse-submodules --depth=1 previous_release_repository

echo "Download clickhouse-server from the previous release"
mkdir previous_release_package_folder

echo $previous_release_tag | download_release_packages && echo -e 'Download script exit code\tOK' >> /test_output/test_results.tsv \
    || echo -e 'Download script failed\tFAIL' >> /test_output/test_results.tsv

# Check if we cloned previous release repository successfully
if ! [ "$(ls -A previous_release_repository/tests/queries)" ]
then
    echo -e "Failed to clone previous release tests\tFAIL" >> /test_output/test_results.tsv
elif ! [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb && ls -A previous_release_package_folder/clickhouse-server_*.deb)" ]
then
    echo -e "Failed to download previous release packages\tFAIL" >> /test_output/test_results.tsv
else
    echo -e "Successfully cloned previous release tests\tOK" >> /test_output/test_results.tsv
    echo -e "Successfully downloaded previous release packages\tOK" >> /test_output/test_results.tsv

    # Make upgrade check more funny by forcing Ordinary engine for system database
    mkdir /var/lib/clickhouse/metadata
    echo "ATTACH DATABASE system ENGINE=Ordinary" > /var/lib/clickhouse/metadata/system.sql

    # Install previous release packages
    install_packages previous_release_package_folder

    # Start server from previous release
    # Let's enable S3 storage by default
    export USE_S3_STORAGE_FOR_MERGE_TREE=1
    # Previous version may not be ready for fault injections
    export ZOOKEEPER_FAULT_INJECTION=0
    configure

    # But we still need default disk because some tables loaded only into it
    sudo cat /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml \
      | sed "s|<main><disk>s3</disk></main>|<main><disk>s3</disk></main><default><disk>default</disk></default>|" \
      > /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp    mv /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
    sudo chown clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
    sudo chgrp clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml

    # Avoid "Setting s3_check_objects_after_upload is neither a builtin setting..."
    rm -f /etc/clickhouse-server/users.d/enable_blobs_check.xml ||:
    rm -f /etc/clickhouse-server/users.d/marks.xml ||:

    # Remove s3 related configs to avoid "there is no disk type `cache`"
    rm -f /etc/clickhouse-server/config.d/storage_conf.xml ||:
    rm -f /etc/clickhouse-server/config.d/azure_storage_conf.xml ||:

    # Turn on after 22.12
    rm -f /etc/clickhouse-server/config.d/compressed_marks_and_index.xml ||:
    # it uses recently introduced settings which previous versions may not have
    rm -f /etc/clickhouse-server/users.d/insert_keeper_retries.xml ||:

    start

    clickhouse-client --query="SELECT 'Server version: ', version()"
    
    mkdir tmp_stress_output

    stress --test-cmd="/usr/bin/clickhouse-test --queries=\"previous_release_repository/tests/queries\""  --upgrade-check --output-folder tmp_stress_output --global-time-limit=1200 \
        && echo -e "Test script exit code$OK" >> /test_output/test_results.tsv \
        || echo -e "Test script failed$FAIL script exit code: $?" >> /test_output/test_results.tsv

    rm -rf tmp_stress_output

    # We experienced deadlocks in this command in very rare cases. Let's debug it:
    timeout 10m clickhouse-client --query="SELECT 'Tables count:', count() FROM system.tables" ||
    (
        echo "thread apply all backtrace (on select tables count)" >> /test_output/gdb.log
        timeout 30m gdb -batch -ex 'thread apply all backtrace' -p "$(cat /var/run/clickhouse-server/clickhouse-server.pid)" | ts '%Y-%m-%d %H:%M:%S' >> /test_output/gdb.log
        clickhouse stop --force
    )

    # Use bigger timeout for previous version
    stop 300
    mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

    # Install and start new server
    install_packages package_folder
    # Disable fault injections on start (we don't test them here, and it can lead to tons of requests in case of huge number of tables).
    export ZOOKEEPER_FAULT_INJECTION=0
    configure
    start 500
    clickhouse-client --query "SELECT 'Server successfully started', 'OK', NULL, ''" >> /test_output/test_results.tsv \
        || (rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt \
        && echo -e "Server failed to start (see application_errors.txt and clickhouse-server.clean.log)$FAIL$(trim_server_logs application_errors.txt)" \
        >> /test_output/test_results.tsv)

    # Remove file application_errors.txt if it's empty
    [ -s /test_output/application_errors.txt ] || rm /test_output/application_errors.txt

    clickhouse-client --query="SELECT 'Server version: ', version()"

    # Let the server run for a while before checking log.
    sleep 60

    stop
    mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.upgrade.log

    # Error messages (we should ignore some errors)
    # FIXME https://github.com/ClickHouse/ClickHouse/issues/38643 ("Unknown index: idx.")
    # FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 ("Cannot parse string 'Hello' as UInt64")
    # FIXME Not sure if it's expected, but some tests from stress test may not be finished yet when we restarting server.
    #       Let's just ignore all errors from queries ("} <Error> TCPHandler: Code:", "} <Error> executeQuery: Code:")
    # FIXME https://github.com/ClickHouse/ClickHouse/issues/39197 ("Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'")
    # NOTE  Incompatibility was introduced in https://github.com/ClickHouse/ClickHouse/pull/39263, it's expected
    #       ("This engine is deprecated and is not supported in transactions", "[Queue = DB::MergeMutateRuntimeQueue]: Code: 235. DB::Exception: Part")
    # FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 - bad mutation does not indicate backward incompatibility
    echo "Check for Error messages in server log:"
    rg -Fav -e "Code: 236. DB::Exception: Cancelled merging parts" \
               -e "Code: 236. DB::Exception: Cancelled mutating parts" \
               -e "REPLICA_IS_ALREADY_ACTIVE" \
               -e "REPLICA_ALREADY_EXISTS" \
               -e "ALL_REPLICAS_LOST" \
               -e "DDLWorker: Cannot parse DDL task query" \
               -e "RaftInstance: failed to accept a rpc connection due to error 125" \
               -e "UNKNOWN_DATABASE" \
               -e "NETWORK_ERROR" \
               -e "UNKNOWN_TABLE" \
               -e "ZooKeeperClient" \
               -e "KEEPER_EXCEPTION" \
               -e "DirectoryMonitor" \
               -e "TABLE_IS_READ_ONLY" \
               -e "Code: 1000, e.code() = 111, Connection refused" \
               -e "UNFINISHED" \
               -e "NETLINK_ERROR" \
               -e "Renaming unexpected part" \
               -e "PART_IS_TEMPORARILY_LOCKED" \
               -e "and a merge is impossible: we didn't find" \
               -e "found in queue and some source parts for it was lost" \
               -e "is lost forever." \
               -e "Unknown index: idx." \
               -e "Cannot parse string 'Hello' as UInt64" \
               -e "} <Error> TCPHandler: Code:" \
               -e "} <Error> executeQuery: Code:" \
               -e "Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'" \
               -e "This engine is deprecated and is not supported in transactions" \
               -e "[Queue = DB::MergeMutateRuntimeQueue]: Code: 235. DB::Exception: Part" \
               -e "The set of parts restored in place of" \
               -e "(ReplicatedMergeTreeAttachThread): Initialization failed. Error" \
               -e "Code: 269. DB::Exception: Destination table is myself" \
               -e "Coordination::Exception: Connection loss" \
               -e "MutateFromLogEntryTask" \
               -e "No connection to ZooKeeper, cannot get shared table ID" \
               -e "Session expired" \
               -e "TOO_MANY_PARTS" \
               -e "Authentication failed" \
        /var/log/clickhouse-server/clickhouse-server.upgrade.log | zgrep -Fa "<Error>" > /test_output/upgrade_error_messages.txt \
        && echo -e "Error message in clickhouse-server.log (see upgrade_error_messages.txt)$FAIL$(head_escaped /test_output/bc_check_error_messages.txt)" \
            >> /test_output/test_results.tsv \
        || echo -e "No Error messages after server upgrade$OK" >> /test_output/test_results.tsv

    # Remove file bc_check_error_messages.txt if it's empty
    [ -s /test_output/upgrade_error_messages.txt ] || rm /test_output/upgrade_error_messages.txt

    # Sanitizer asserts
    rg -Fa "==================" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
    rg -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
    rg -Fav -e "ASan doesn't fully support makecontext/swapcontext functions" -e "DB::Exception" /test_output/tmp > /dev/null \
        && echo -e "Sanitizer assert (in stderr.log)$FAIL$(head_escaped /test_output/tmp)" >> /test_output/test_results.tsv \
        || echo -e "No sanitizer asserts$OK" >> /test_output/test_results.tsv
    rm -f /test_output/tmp

    # OOM
    rg -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server.*.log > /dev/null \
        && echo -e "Signal 9 in clickhouse-server.log$FAIL" >> /test_output/test_results.tsv \
        || echo -e "No OOM messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

    # Logical errors
    echo "Check for Logical errors in server log:"
    rg -Fa -A20 "Code: 49, e.displayText() = DB::Exception:" /var/log/clickhouse-server/clickhouse-server.*.log > /test_output/logical_errors.txt \
        && echo -e "Logical error thrown (see clickhouse-server.log or logical_errors.txt)$FAIL$(head_escaped /test_output/logical_errors.txt)" >> /test_output/test_results.tsv \
        || echo -e "No logical errors$OK" >> /test_output/test_results.tsv

    # Remove file logical_errors.txt if it's empty
    [ -s /test_output/logical_errors.txt ] || rm /test_output/logical_errors.txt

    # Crash
    rg -Fa "########################################" /var/log/clickhouse-server/clickhouse-server.*.log > /dev/null \
        && echo -e "Killed by signal (in clickhouse-server.log)$FAIL" >> /test_output/test_results.tsv \
        || echo -e "Not crashed$OK" >> /test_output/test_results.tsv

    # It also checks for crash without stacktrace (printed by watchdog)
    echo "Check for Fatal message in server log:"
    rg -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.*.log > /test_output/fatal_messages.txt \
        && echo -e "Fatal message in clickhouse-server.log (see fatal_messages.txt)$FAIL$(trim_server_logs fatal_messages.txt)" >> /test_output/test_results.tsv \
        || echo -e "No fatal messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

    # Remove file fatal_messages.txt if it's empty
    [ -s /test_output/fatal_messages.txt ] || rm /test_output/fatal_messages.txt

    rg -Fa "########################################" /test_output/* > /dev/null \
        && echo -e "Killed by signal (output files)$FAIL" >> /test_output/test_results.tsv

    tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:
    for table in query_log trace_log
    do
        clickhouse-local --path /var/lib/clickhouse/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" \
          | zstd --threads=0 > /test_output/$table.tsv.zst ||:
    done
fi

dmesg -T > /test_output/dmesg.log

# OOM in dmesg -- those are real
grep -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' /test_output/dmesg.log \
    && echo -e "OOM in dmesg$FAIL$(head_escaped /test_output/dmesg.log)" >> /test_output/test_results.tsv \
    || echo -e "No OOM in dmesg$OK" >> /test_output/test_results.tsv

mv /var/log/clickhouse-server/stderr.log /test_output/

# If we failed to clone repo or download previous release packages,
# we don't have any packages installed, but we need clickhouse-local
# to be installed to create check_status.tsv.
if ! command -v clickhouse-local &> /dev/null
then
    install_packages package_folder
fi

# Write check result into check_status.tsv
# Try to choose most specific error for the whole check status
clickhouse-local --structure "test String, res String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by
(test like '%Sanitizer%') DESC,
(test like '%Killed by signal%') DESC,
(test like '%gdb.log%') DESC,
(test ilike '%possible deadlock%') DESC,
(test like '%start%') DESC,
(test like '%dmesg%') DESC,
(test like '%OOM%') DESC,
(test like '%Signal 9%') DESC,
(test like '%Fatal message%') DESC,
(test like '%Error message%') DESC,
(test like '%previous release%') DESC,
rowNumberInAllBlocks()
LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv

# Core dumps
find . -type f -maxdepth 1 -name 'core.*' | while read core; do
    zstd --threads=0 $core
    mv $core.zst /test_output/
done
