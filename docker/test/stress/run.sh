#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

set -x

# Thread Fuzzer allows to check more permutations of possible thread scheduling
# and find more potential issues.

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
    /usr/share/clickhouse-test/config/install.sh

    # we mount tests folder from repo to /usr/share
    ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test

    # avoid too slow startup
    sudo cat /etc/clickhouse-server/config.d/keeper_port.xml | sed "s|<snapshot_distance>100000</snapshot_distance>|<snapshot_distance>10000</snapshot_distance>|" > /etc/clickhouse-server/config.d/keeper_port.xml.tmp
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
    local max_server_mem
    max_server_mem=$((total_mem*75/100)) # 75%
    echo "Setting max_server_memory_usage=$max_server_mem"
    cat > /etc/clickhouse-server/config.d/max_server_memory_usage.xml <<EOL
<clickhouse>
    <max_server_memory_usage>${max_server_mem}</max_server_memory_usage>
</clickhouse>
EOL
    local max_users_mem
    max_users_mem=$((total_mem*50/100)) # 50%
    echo "Setting max_memory_usage_for_user=$max_users_mem"
    cat > /etc/clickhouse-server/users.d/max_memory_usage_for_user.xml <<EOL
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage_for_user>${max_users_mem}</max_memory_usage_for_user>
        </default>
    </profiles>
</clickhouse>
EOL
}

function stop()
{
    clickhouse stop
}

function start()
{
    counter=0
    until clickhouse-client --query "SELECT 1"
    do
        if [ "$counter" -gt ${1:-240} ]
        then
            echo "Cannot start clickhouse-server"
            cat /var/log/clickhouse-server/stdout.log
            tail -n1000 /var/log/clickhouse-server/stderr.log
            tail -n100000 /var/log/clickhouse-server/clickhouse-server.log | grep -F -v -e '<Warning> RaftInstance:' -e '<Information> RaftInstance' | tail -n1000
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
gcore
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

install_packages package_folder

configure

./setup_minio.sh

start

# shellcheck disable=SC2086 # No quotes because I want to split it into words.
/s3downloader --url-prefix "$S3_URL" --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.initial.log

start

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
clickhouse-client --query "CREATE TABLE test.hits_s3  (WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
clickhouse-client --query "INSERT INTO test.hits_s3 SELECT * FROM test.hits"
clickhouse-client --query "SHOW TABLES FROM test"

./stress --hung-check --drop-databases --output-folder test_output --skip-func-tests "$SKIP_TESTS_OPTION" \
    && echo -e 'Test script exit code\tOK' >> /test_output/test_results.tsv \
    || echo -e 'Test script failed\tFAIL' >> /test_output/test_results.tsv

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

start

clickhouse-client --query "SELECT 'Server successfully started', 'OK'" >> /test_output/test_results.tsv \
                       || (echo -e 'Server failed to start (see application_errors.txt)\tFAIL' >> /test_output/test_results.tsv \
                       && grep -Fa "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt)

[ -f /var/log/clickhouse-server/clickhouse-server.log ] || echo -e "Server log does not exist\tFAIL"
[ -f /var/log/clickhouse-server/stderr.log ] || echo -e "Stderr log does not exist\tFAIL"

# Grep logs for sanitizer asserts, crashes and other critical errors

# Sanitizer asserts
grep -Fa "==================" /var/log/clickhouse-server/stderr.log | grep -v "in query:" >> /test_output/tmp
grep -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
zgrep -Fav "ASan doesn't fully support makecontext/swapcontext functions" /test_output/tmp > /dev/null \
    && echo -e 'Sanitizer assert (in stderr.log)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No sanitizer asserts\tOK' >> /test_output/test_results.tsv
rm -f /test_output/tmp

# OOM
zgrep -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server*.log > /dev/null \
    && echo -e 'OOM killer (or signal 9) in clickhouse-server.log\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No OOM messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

# Logical errors
zgrep -Fa "Code: 49, e.displayText() = DB::Exception:" /var/log/clickhouse-server/clickhouse-server*.log > /test_output/logical_errors.txt \
    && echo -e 'Logical error thrown (see clickhouse-server.log or logical_errors.txt)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No logical errors\tOK' >> /test_output/test_results.tsv

# Remove file logical_errors.txt if it's empty
[ -s /test_output/logical_errors.txt ] || rm /test_output/logical_errors.txt

# Crash
zgrep -Fa "########################################" /var/log/clickhouse-server/clickhouse-server*.log > /dev/null \
    && echo -e 'Killed by signal (in clickhouse-server.log)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'Not crashed\tOK' >> /test_output/test_results.tsv

# It also checks for crash without stacktrace (printed by watchdog)
zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server*.log > /test_output/fatal_messages.txt \
    && echo -e 'Fatal message in clickhouse-server.log (see fatal_messages.txt)\tFAIL' >> /test_output/test_results.tsv \
    || echo -e 'No fatal messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

# Remove file fatal_messages.txt if it's empty
[ -s /test_output/fatal_messages.txt ] || rm /test_output/fatal_messages.txt

zgrep -Fa "########################################" /test_output/* > /dev/null \
    && echo -e 'Killed by signal (output files)\tFAIL' >> /test_output/test_results.tsv

zgrep -Fa " received signal " /test_output/gdb.log > /dev/null \
    && echo -e 'Found signal in gdb.log\tFAIL' >> /test_output/test_results.tsv

echo -e "Backward compatibility check\n"

echo "Download previous release server"
mkdir previous_release_package_folder
clickhouse-client --query="SELECT version()" | ./download_previous_release && echo -e 'Download script exit code\tOK' >> /test_output/test_results.tsv \
    || echo -e 'Download script failed\tFAIL' >> /test_output/test_results.tsv

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.clean.log

if [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb && ls -A previous_release_package_folder/clickhouse-server_*.deb)" ]
then
    echo -e "Successfully downloaded previous release packets\tOK" >> /test_output/test_results.tsv

    # Uninstall current packages
    dpkg --remove clickhouse-client
    dpkg --remove clickhouse-server
    dpkg --remove clickhouse-common-static-dbg
    dpkg --remove clickhouse-common-static

    rm -rf /var/lib/clickhouse/*

    # Install previous release packages
    install_packages previous_release_package_folder

    # Start server from previous release
    configure
    start

    clickhouse-client --query="SELECT 'Server version: ', version()"

    # Install new package before running stress test because we should use new clickhouse-client and new clickhouse-test
    install_packages package_folder

    mkdir tmp_stress_output

    ./stress --backward-compatibility-check --output-folder tmp_stress_output --global-time-limit=1200 \
        && echo -e 'Backward compatibility check: Test script exit code\tOK' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: Test script failed\tFAIL' >> /test_output/test_results.tsv
    rm -rf tmp_stress_output

    clickhouse-client --query="SELECT 'Tables count:', count() FROM system.tables"

    stop
    mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.backward.stress.log

    # Start new server
    configure
    start 500
    clickhouse-client --query "SELECT 'Backward compatibility check: Server successfully started', 'OK'" >> /test_output/test_results.tsv \
        || (echo -e 'Backward compatibility check: Server failed to start\tFAIL' >> /test_output/test_results.tsv \
        && grep -Fa "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log >> /test_output/bc_check_application_errors.txt)

    clickhouse-client --query="SELECT 'Server version: ', version()"

    # Let the server run for a while before checking log.
    sleep 60

    stop
    mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.backward.clean.log

    # Error messages (we should ignore some errors)
    echo "Check for Error messages in server log:"
    zgrep -Fav -e "Code: 236. DB::Exception: Cancelled merging parts" \
               -e "Code: 236. DB::Exception: Cancelled mutating parts" \
               -e "REPLICA_IS_ALREADY_ACTIVE" \
               -e "REPLICA_IS_ALREADY_EXIST" \
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
               -e "Renaming unexpected part" \
        /var/log/clickhouse-server/clickhouse-server.backward.clean.log | zgrep -Fa "<Error>" > /test_output/bc_check_error_messages.txt \
        && echo -e 'Backward compatibility check: Error message in clickhouse-server.log (see bc_check_error_messages.txt)\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: No Error messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

    # Remove file bc_check_error_messages.txt if it's empty
    [ -s /test_output/bc_check_error_messages.txt ] || rm /test_output/bc_check_error_messages.txt

    # Sanitizer asserts
    zgrep -Fa "==================" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
    zgrep -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
    zgrep -Fav "ASan doesn't fully support makecontext/swapcontext functions" /test_output/tmp > /dev/null \
        && echo -e 'Backward compatibility check: Sanitizer assert (in stderr.log)\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: No sanitizer asserts\tOK' >> /test_output/test_results.tsv
    rm -f /test_output/tmp

    # OOM
    zgrep -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /dev/null \
        && echo -e 'Backward compatibility check: OOM killer (or signal 9) in clickhouse-server.log\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: No OOM messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

    # Logical errors
    echo "Check for Logical errors in server log:"
    zgrep -Fa -A20 "Code: 49, e.displayText() = DB::Exception:" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /test_output/bc_check_logical_errors.txt \
        && echo -e 'Backward compatibility check: Logical error thrown (see clickhouse-server.log or bc_check_logical_errors.txt)\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: No logical errors\tOK' >> /test_output/test_results.tsv

    # Remove file bc_check_logical_errors.txt if it's empty
    [ -s /test_output/bc_check_logical_errors.txt ] || rm /test_output/bc_check_logical_errors.txt

    # Crash
    zgrep -Fa "########################################" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /dev/null \
        && echo -e 'Backward compatibility check: Killed by signal (in clickhouse-server.log)\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: Not crashed\tOK' >> /test_output/test_results.tsv

    # It also checks for crash without stacktrace (printed by watchdog)
    echo "Check for Fatal message in server log:"
    zgrep -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.backward.*.log > /test_output/bc_check_fatal_messages.txt \
        && echo -e 'Backward compatibility check: Fatal message in clickhouse-server.log (see bc_check_fatal_messages.txt)\tFAIL' >> /test_output/test_results.tsv \
        || echo -e 'Backward compatibility check: No fatal messages in clickhouse-server.log\tOK' >> /test_output/test_results.tsv

    # Remove file bc_check_fatal_messages.txt if it's empty
    [ -s /test_output/bc_check_fatal_messages.txt ] || rm /test_output/bc_check_fatal_messages.txt
else
    echo -e "Backward compatibility check: Failed to download previous release packets\tFAIL" >> /test_output/test_results.tsv
fi

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:
mv /var/log/clickhouse-server/stderr.log /test_output/

# Replace the engine with Ordinary to avoid extra symlinks stuff in artifacts.
# (so that clickhouse-local --path can read it w/o extra care).
sed -i -e "s/ATTACH DATABASE _ UUID '[^']*'/ATTACH DATABASE system/" -e "s/Atomic/Ordinary/" /var/lib/clickhouse/metadata/system.sql
for table in query_log trace_log; do
    sed -i "s/ATTACH TABLE _ UUID '[^']*'/ATTACH TABLE $table/" /var/lib/clickhouse/metadata/system/${table}.sql
    tar -chf /test_output/${table}_dump.tar /var/lib/clickhouse/metadata/system.sql /var/lib/clickhouse/metadata/system/${table}.sql /var/lib/clickhouse/data/system/${table} ||:
done

# Write check result into check_status.tsv
clickhouse-local --structure "test String, res String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by (lower(test) like '%hung%') LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv

# Core dumps (see gcore)
# Default filename is 'core.PROCESS_ID'
for core in core.*; do
    pigz $core
    mv $core.gz /test_output/
done
