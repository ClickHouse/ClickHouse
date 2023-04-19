#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

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

    # we mount tests folder from repo to /usr/share
    ln -s /usr/share/clickhouse-test/clickhouse-test /usr/bin/clickhouse-test
    ln -s /usr/share/clickhouse-test/ci/download_release_packages.py /usr/bin/download_release_packages
    ln -s /usr/share/clickhouse-test/ci/get_previous_release_tag.py /usr/bin/get_previous_release_tag

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

install_packages package_folder

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

export ZOOKEEPER_FAULT_INJECTION=1
configure

azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --debug /azurite_log &
./setup_minio.sh stateless # to have a proper environment

start

shellcheck disable=SC2086 # No quotes because I want to split it into words.
/s3downloader --url-prefix "$S3_URL" --dataset-names $DATASETS
chmod 777 -R /var/lib/clickhouse
clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.initial.log

start

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"

clickhouse-client --query "CREATE TABLE test.hits_s3 (WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,
    EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,
    UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,
    Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),
    RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8,
    FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,  UserAgentMinor FixedString(2),
    CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,
    IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,
    WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,
    IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,
    IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,
    Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), RemoteIP UInt32,
    RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16, SendTiming Int32,
    DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,
    RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,
    RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,
    ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,
    OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,
    UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,
    URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,
    ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),
    IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
clickhouse-client --query "CREATE TABLE test.hits (WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,
    EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,
    UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,
    RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),
    URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,
    FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,
    MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,
    SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,
    ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,
    FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8,
    HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,
    GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,
    HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,
    HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,
    FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,
    RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,
    ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,
    OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,
    UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,
    URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,
    ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),
    IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate)
    ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
clickhouse-client --query "CREATE TABLE test.visits (CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8,
    VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32,
    Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String,
    EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String,
    AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32),
    RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32,
    SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32,
    ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32,
    SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16,
    UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16,
    FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8,
    FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8,
    Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8,
    BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16),
    Params Array(String),  Goals Nested(ID UInt32, Serial UInt32, EventTime DateTime,  Price Int64,  OrderID String, CurrencyID UInt32),
    WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64,
    ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32,
    ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32,
    ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32,
    ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16,
    ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32,
    OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String,
    UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime,
    PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  TraficSource    Nested(ID Int8,  SearchEngineID UInt16, AdvEngineID UInt8,
    PlaceID UInt16, SocialSourceNetworkID UInt8, Domain String, SearchPhrase String, SocialSourcePage String),  Attendance FixedString(16),
    CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64,
    StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64,
    OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64,
    UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32,
    ParsedParams    Nested(Key1 String,  Key2 String,  Key3 String,  Key4 String, Key5 String, ValueDouble    Float64),
    Market Nested(Type UInt8, GoalID UInt32, OrderID String,  OrderPrice Int64,  PP UInt32,  DirectPlaceID UInt32,  DirectOrderID  UInt32,
    DirectBannerID UInt32,  GoodID String, GoodName String, GoodQuantity Int32,  GoodPrice Int64),  IslandID FixedString(16))
    ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
    SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"

clickhouse-client --query "INSERT INTO test.hits_s3 SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0"
clickhouse-client --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0"
clickhouse-client --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0"

clickhouse-client --query "DROP TABLE datasets.visits_v1 SYNC"
clickhouse-client --query "DROP TABLE datasets.hits_v1 SYNC"

clickhouse-client --query "SHOW TABLES FROM test"

clickhouse-client --query "SYSTEM STOP THREAD FUZZER"

stop

# Let's enable S3 storage by default
export USE_S3_STORAGE_FOR_MERGE_TREE=1
export ZOOKEEPER_FAULT_INJECTION=1
configure

# But we still need default disk because some tables loaded only into it
sudo cat /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml \
  | sed "s|<main><disk>s3</disk></main>|<main><disk>s3</disk></main><default><disk>default</disk></default>|" \
  > /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp
mv /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chown clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
sudo chgrp clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml

start

./stress --hung-check --drop-databases --output-folder test_output --skip-func-tests "$SKIP_TESTS_OPTION" --global-time-limit 1200 \
    && echo -e "Test script exit code$OK" >> /test_output/test_results.tsv \
    || echo -e "Test script failed$FAIL script exit code: $?" >> /test_output/test_results.tsv

# NOTE Hung check is implemented in docker/tests/stress/stress
rg -Fa "No queries hung" /test_output/test_results.tsv | grep -Fa "OK" \
  || echo -e "Hung check failed, possible deadlock found (see hung_check.log)$FAIL$(head_escaped /test_output/hung_check.log)" >> /test_output/test_results.tsv

stop
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

# NOTE Disable thread fuzzer before server start with data after stress test.
# In debug build it can take a lot of time.
unset "${!THREAD_@}"

start

clickhouse-client --query "SELECT 'Server successfully started', 'OK', NULL, ''" >> /test_output/test_results.tsv \
    || (rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log > /test_output/application_errors.txt \
    && echo -e "Server failed to start (see application_errors.txt and clickhouse-server.clean.log)$FAIL$(trim_server_logs application_errors.txt)" \
    >> /test_output/test_results.tsv)

stop

[ -f /var/log/clickhouse-server/clickhouse-server.log ] || echo -e "Server log does not exist\tFAIL"
[ -f /var/log/clickhouse-server/stderr.log ] || echo -e "Stderr log does not exist\tFAIL"

# Grep logs for sanitizer asserts, crashes and other critical errors

# Sanitizer asserts
rg -Fa "==================" /var/log/clickhouse-server/stderr.log | rg -v "in query:" >> /test_output/tmp
rg -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
rg -Fav -e "ASan doesn't fully support makecontext/swapcontext functions" -e "DB::Exception" /test_output/tmp > /dev/null \
    && echo -e "Sanitizer assert (in stderr.log)$FAIL$(head_escaped /test_output/tmp)" >> /test_output/test_results.tsv \
    || echo -e "No sanitizer asserts$OK" >> /test_output/test_results.tsv
rm -f /test_output/tmp

# OOM
rg -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server*.log > /dev/null \
    && echo -e "Signal 9 in clickhouse-server.log$FAIL" >> /test_output/test_results.tsv \
    || echo -e "No OOM messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

# Logical errors
rg -Fa "Code: 49. DB::Exception: " /var/log/clickhouse-server/clickhouse-server*.log > /test_output/logical_errors.txt \
    && echo -e "Logical error thrown (see clickhouse-server.log or logical_errors.txt)$FAIL$(head_escaped /test_output/logical_errors.txt)" >> /test_output/test_results.tsv \
    || echo -e "No logical errors$OK" >> /test_output/test_results.tsv

# Remove file logical_errors.txt if it's empty
[ -s /test_output/logical_errors.txt ] || rm /test_output/logical_errors.txt

# No such key errors
rg --text "Code: 499.*The specified key does not exist" /var/log/clickhouse-server/clickhouse-server*.log > /test_output/no_such_key_errors.txt \
    && echo -e "S3_ERROR No such key thrown (see clickhouse-server.log or no_such_key_errors.txt)$FAIL$(trim_server_logs no_such_key_errors.txt)" >> /test_output/test_results.tsv \
    || echo -e "No lost s3 keys$OK" >> /test_output/test_results.tsv

# Remove file no_such_key_errors.txt if it's empty
[ -s /test_output/no_such_key_errors.txt ] || rm /test_output/no_such_key_errors.txt

# Crash
rg -Fa "########################################" /var/log/clickhouse-server/clickhouse-server*.log > /dev/null \
    && echo -e "Killed by signal (in clickhouse-server.log)$FAIL" >> /test_output/test_results.tsv \
    || echo -e "Not crashed$OK" >> /test_output/test_results.tsv

# It also checks for crash without stacktrace (printed by watchdog)
rg -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server*.log > /test_output/fatal_messages.txt \
    && echo -e "Fatal message in clickhouse-server.log (see fatal_messages.txt)$FAIL$(trim_server_logs fatal_messages.txt)" >> /test_output/test_results.tsv \
    || echo -e "No fatal messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

# Remove file fatal_messages.txt if it's empty
[ -s /test_output/fatal_messages.txt ] || rm /test_output/fatal_messages.txt

rg -Fa "########################################" /test_output/* > /dev/null \
    && echo -e "Killed by signal (output files)$FAIL" >> /test_output/test_results.tsv

function get_gdb_log_context()
{
    rg -A50 -Fa " received signal " /test_output/gdb.log | head_escaped
}

rg -Fa " received signal " /test_output/gdb.log > /dev/null \
    && echo -e "Found signal in gdb.log$FAIL$(get_gdb_log_context)" >> /test_output/test_results.tsv

if [ "$DISABLE_BC_CHECK" -ne "1" ]; then
    echo -e "Backward compatibility check\n"

    echo "Get previous release tag"
    previous_release_tag=$(clickhouse-client --version | rg -o "[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*" | get_previous_release_tag)
    echo $previous_release_tag

    echo "Clone previous release repository"
    git clone https://github.com/ClickHouse/ClickHouse.git --no-tags --progress --branch=$previous_release_tag --no-recurse-submodules --depth=1 previous_release_repository

    echo "Download clickhouse-server from the previous release"
    mkdir previous_release_package_folder

    echo $previous_release_tag | download_release_packages && echo -e "Download script exit code$OK" >> /test_output/test_results.tsv \
        || echo -e "Download script failed$FAIL" >> /test_output/test_results.tsv

    mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.clean.log
    for table in query_log trace_log
    do
        clickhouse-local --path /var/lib/clickhouse/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" | zstd --threads=0 > /test_output/$table.tsv.zst ||:
    done

    tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

    # Check if we cloned previous release repository successfully
    if ! [ "$(ls -A previous_release_repository/tests/queries)" ]
    then
        echo -e "Backward compatibility check: Failed to clone previous release tests$FAIL" >> /test_output/test_results.tsv
    elif ! [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb && ls -A previous_release_package_folder/clickhouse-server_*.deb)" ]
    then
        echo -e "Backward compatibility check: Failed to download previous release packages$FAIL" >> /test_output/test_results.tsv
    else
        echo -e "Successfully cloned previous release tests$OK" >> /test_output/test_results.tsv
        echo -e "Successfully downloaded previous release packages$OK" >> /test_output/test_results.tsv

        # Uninstall current packages
        dpkg --remove clickhouse-client
        dpkg --remove clickhouse-server
        dpkg --remove clickhouse-common-static-dbg
        dpkg --remove clickhouse-common-static

        rm -rf /var/lib/clickhouse/*

        # Make BC check more funny by forcing Ordinary engine for system database
        mkdir /var/lib/clickhouse/metadata
        echo "ATTACH DATABASE system ENGINE=Ordinary" > /var/lib/clickhouse/metadata/system.sql

        # Install previous release packages
        install_packages previous_release_package_folder

        # Start server from previous release
        # Previous version may not be ready for fault injections
        export ZOOKEEPER_FAULT_INJECTION=0
        configure

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

        # Turn on after 23.1
        rm -f /etc/clickhouse-server/users.d/prefetch_settings.xml ||:

        start

        clickhouse-client --query="SELECT 'Server version: ', version()"

        # Install new package before running stress test because we should use new
        # clickhouse-client and new clickhouse-test.
        #
        # But we should leave old binary in /usr/bin/ and debug symbols in
        # /usr/lib/debug/usr/bin (if any) for gdb and internal DWARF parser, so it
        # will print sane stacktraces and also to avoid possible crashes.
        #
        # FIXME: those files can be extracted directly from debian package, but
        # actually better solution will be to use different PATH instead of playing
        # games with files from packages.
        mv /usr/bin/clickhouse previous_release_package_folder/
        mv /usr/lib/debug/usr/bin/clickhouse.debug previous_release_package_folder/
        install_packages package_folder
        mv /usr/bin/clickhouse package_folder/
        mv /usr/lib/debug/usr/bin/clickhouse.debug package_folder/
        mv previous_release_package_folder/clickhouse /usr/bin/
        mv previous_release_package_folder/clickhouse.debug /usr/lib/debug/usr/bin/clickhouse.debug

        mkdir tmp_stress_output

        ./stress --test-cmd="/usr/bin/clickhouse-test --queries=\"previous_release_repository/tests/queries\""  \
            --backward-compatibility-check --output-folder tmp_stress_output --global-time-limit=1200 \
            && echo -e "Backward compatibility check: Test script exit code$OK" >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: Test script failed$FAIL" >> /test_output/test_results.tsv
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
        mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.backward.stress.log

        # Start new server
        mv package_folder/clickhouse /usr/bin/
        mv package_folder/clickhouse.debug /usr/lib/debug/usr/bin/clickhouse.debug
        # Disable fault injections on start (we don't test them here, and it can lead to tons of requests in case of huge number of tables).
        export ZOOKEEPER_FAULT_INJECTION=0
        configure
        start 500
        clickhouse-client --query "SELECT 'Backward compatibility check: Server successfully started', 'OK', NULL, ''" >> /test_output/test_results.tsv \
            || (rg --text "<Error>.*Application" /var/log/clickhouse-server/clickhouse-server.log >> /test_output/bc_check_application_errors.txt \
            && echo -e "Backward compatibility check: Server failed to start$FAIL$(trim_server_logs bc_check_application_errors.txt)" >> /test_output/test_results.tsv)

        clickhouse-client --query="SELECT 'Server version: ', version()"

        # Let the server run for a while before checking log.
        sleep 60

        stop
        mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.backward.dirty.log

        # Error messages (we should ignore some errors)
        # FIXME https://github.com/ClickHouse/ClickHouse/issues/38643 ("Unknown index: idx.")
        # FIXME https://github.com/ClickHouse/ClickHouse/issues/39174 ("Cannot parse string 'Hello' as UInt64")
        # FIXME Not sure if it's expected, but some tests from BC check may not be finished yet when we restarting server.
        #       Let's just ignore all errors from queries ("} <Error> TCPHandler: Code:", "} <Error> executeQuery: Code:")
        # FIXME https://github.com/ClickHouse/ClickHouse/issues/39197 ("Missing columns: 'v3' while processing query: 'v3, k, v1, v2, p'")
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
                   -e "[Queue = DB::DynamicRuntimeQueue]: Code: 235. DB::Exception: Part" \
                   -e "The set of parts restored in place of" \
                   -e "(ReplicatedMergeTreeAttachThread): Initialization failed. Error" \
                   -e "Code: 269. DB::Exception: Destination table is myself" \
                   -e "Coordination::Exception: Connection loss" \
                   -e "MutateFromLogEntryTask" \
                   -e "No connection to ZooKeeper, cannot get shared table ID" \
                   -e "Session expired" \
                   -e "TOO_MANY_PARTS" \
                   -e "Container already exists" \
            /var/log/clickhouse-server/clickhouse-server.backward.dirty.log | rg -Fa "<Error>" > /test_output/bc_check_error_messages.txt \
            && echo -e "Backward compatibility check: Error message in clickhouse-server.log (see bc_check_error_messages.txt)$FAIL$(trim_server_logs bc_check_error_messages.txt)" \
                >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: No Error messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

        # Remove file bc_check_error_messages.txt if it's empty
        [ -s /test_output/bc_check_error_messages.txt ] || rm /test_output/bc_check_error_messages.txt

        # Sanitizer asserts
        rg -Fa "==================" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
        rg -Fa "WARNING" /var/log/clickhouse-server/stderr.log >> /test_output/tmp
        rg -Fav -e "ASan doesn't fully support makecontext/swapcontext functions" -e "DB::Exception" /test_output/tmp > /dev/null \
            && echo -e "Backward compatibility check: Sanitizer assert (in stderr.log)$FAIL$(head_escaped /test_output/tmp)" >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: No sanitizer asserts$OK" >> /test_output/test_results.tsv
        rm -f /test_output/tmp

        # OOM
        rg -Fa " <Fatal> Application: Child process was terminated by signal 9" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /dev/null \
            && echo -e "Backward compatibility check: Signal 9 in clickhouse-server.log$FAIL" >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: No OOM messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

        # Logical errors
        echo "Check for Logical errors in server log:"
        rg -Fa -A20 "Code: 49. DB::Exception:" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /test_output/bc_check_logical_errors.txt \
            && echo -e "Backward compatibility check: Logical error thrown (see clickhouse-server.log or bc_check_logical_errors.txt)$FAIL$(trim_server_logs bc_check_logical_errors.txt)" \
                >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: No logical errors$OK" >> /test_output/test_results.tsv

        # Remove file bc_check_logical_errors.txt if it's empty
        [ -s /test_output/bc_check_logical_errors.txt ] || rm /test_output/bc_check_logical_errors.txt

        # Crash
        rg -Fa "########################################" /var/log/clickhouse-server/clickhouse-server.backward.*.log > /dev/null \
            && echo -e "Backward compatibility check: Killed by signal (in clickhouse-server.log)$FAIL" >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: Not crashed$OK" >> /test_output/test_results.tsv

        # It also checks for crash without stacktrace (printed by watchdog)
        echo "Check for Fatal message in server log:"
        rg -Fa " <Fatal> " /var/log/clickhouse-server/clickhouse-server.backward.*.log > /test_output/bc_check_fatal_messages.txt \
            && echo -e "Backward compatibility check: Fatal message in clickhouse-server.log (see bc_check_fatal_messages.txt)$FAIL$(trim_server_logs bc_check_fatal_messages.txt)" \
                >> /test_output/test_results.tsv \
            || echo -e "Backward compatibility check: No fatal messages in clickhouse-server.log$OK" >> /test_output/test_results.tsv

        # Remove file bc_check_fatal_messages.txt if it's empty
        [ -s /test_output/bc_check_fatal_messages.txt ] || rm /test_output/bc_check_fatal_messages.txt

        tar -chf /test_output/coordination.backward.tar /var/lib/clickhouse/coordination ||:
        for table in query_log trace_log
        do
            clickhouse-local --path /var/lib/clickhouse/ --only-system-tables -q "select * from system.$table format TSVWithNamesAndTypes" \
              | zstd --threads=0 > /test_output/$table.backward.tsv.zst ||:
        done
    fi
fi

dmesg -T > /test_output/dmesg.log

# OOM in dmesg -- those are real
grep -q -F -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' /test_output/dmesg.log \
    && echo -e "OOM in dmesg$FAIL$(head_escaped /test_output/dmesg.log)" >> /test_output/test_results.tsv \
    || echo -e "No OOM in dmesg$OK" >> /test_output/test_results.tsv

mv /var/log/clickhouse-server/stderr.log /test_output/

# Write check result into check_status.tsv
# Try to choose most specific error for the whole check status
clickhouse-local --structure "test String, res String, time Nullable(Float32), desc String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by
(test like 'Backward compatibility check%'),  -- BC check goes last
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
LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv || echo "failure\tCannot parse test_results.tsv" > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv

# Core dumps
find . -type f -maxdepth 1 -name 'core.*' | while read core; do
    zstd --threads=0 $core
    mv $core.zst /test_output/
done
