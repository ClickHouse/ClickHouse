#!/bin/bash
# shellcheck disable=SC2094
# shellcheck disable=SC2086
# shellcheck disable=SC2024

set -x

# Avoid overlaps with previous runs
dmesg --clear
# shellcheck disable=SC1091
source /setup_export_logs.sh

ln -s /repo/tests/clickhouse-test /usr/bin/clickhouse-test

# Stress tests and upgrade check uses similar code that was placed
# in a separate bash library. See tests/ci/stress_tests.lib
# shellcheck source=../stateless/attach_gdb.lib
source /repo/tests/docker_scripts/attach_gdb.lib
# shellcheck source=../stateless/stress_tests.lib
source /repo/tests/docker_scripts/stress_tests.lib

# shellcheck disable=SC1091
source /repo/tests/docker_scripts/utils.lib

install_packages package_folder

# Thread Fuzzer allows to check more permutations of possible thread scheduling
# and find more potential issues.
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

export THREAD_FUZZER_EXPLICIT_SLEEP_PROBABILITY=0.01
export THREAD_FUZZER_EXPLICIT_MEMORY_EXCEPTION_PROBABILITY=0.01

export ZOOKEEPER_FAULT_INJECTION=1
# Initial run without S3 to create system.*_log on local file system to make it
# available for dump via clickhouse-local
configure

/repo/tests/docker_scripts/setup_minio.sh stateless # to have a proper environment

config_logs_export_cluster /etc/clickhouse-server/config.d/system_logs_export.yaml

start_server

setup_logs_replication

clickhouse-client --query "CREATE DATABASE datasets"
clickhouse-client < /repo/tests/docker_scripts/create.sql
clickhouse-client --query "SHOW TABLES FROM datasets"

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test"

stop_server
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.initial.log

# Randomize cache policies.
cache_policy=""
if [ $((RANDOM % 2)) -eq 1 ]; then
    cache_policy="SLRU"
else
    cache_policy="LRU"
fi

echo "Using cache policy: $cache_policy"

if [ "$cache_policy" = "SLRU" ]; then
    sudo cat /etc/clickhouse-server/config.d/storage_conf.xml \
    | sed "s|<cache_policy>LRU</cache_policy>|<cache_policy>SLRU</cache_policy>|" \
    > /etc/clickhouse-server/config.d/storage_conf.xml.tmp
    mv /etc/clickhouse-server/config.d/storage_conf.xml.tmp /etc/clickhouse-server/config.d/storage_conf.xml
fi

# Disable experimental WINDOW VIEW tests for stress tests, since they may be
# created with old analyzer and then, after server restart it will refuse to
# start.
# FIXME: remove once the support for WINDOW VIEW will be implemented in analyzer.
sudo cat /etc/clickhouse-server/users.d/stress_tests_overrides.xml <<EOL
<clickhouse>
    <profiles>
        <default>
            <allow_experimental_window_view>false</allow_experimental_window_view>
            <constraints>
               <allow_experimental_window_view>
                   <readonly/>
               </allow_experimental_window_view>
            </constraints>
        </default>
    </profiles>
</clickhouse>
EOL

start_server

clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM test"

if [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    TEMP_POLICY="s3_cache"
elif [[ "$USE_AZURE_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    TEMP_POLICY="azure_cache"
else
    TEMP_POLICY="default"
fi


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
    ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='$TEMP_POLICY'"
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
    ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='$TEMP_POLICY'"
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
    SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='$TEMP_POLICY'"

clickhouse-client --query "INSERT INTO test.hits_s3 SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
clickhouse-client --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
clickhouse-client --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"

clickhouse-client --query "DROP TABLE datasets.visits_v1 SYNC"
clickhouse-client --query "DROP TABLE datasets.hits_v1 SYNC"

clickhouse-client --query "SHOW TABLES FROM test"

clickhouse-client --query "SYSTEM STOP THREAD FUZZER"

stop_server

# Let's enable S3 storage by default
export RANDOMIZE_OBJECT_KEY_TYPE=1
export ZOOKEEPER_FAULT_INJECTION=1
export THREAD_POOL_FAULT_INJECTION=1
configure

if [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    # But we still need default disk because some tables loaded only into it
    sudo cat /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml \
      | sed "s|<main><disk>s3</disk></main>|<main><disk>s3</disk></main><default><disk>default</disk></default>|" \
      > /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp
    mv /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml.tmp /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
    sudo chown clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
    sudo chgrp clickhouse /etc/clickhouse-server/config.d/s3_storage_policy_by_default.xml
elif [[ "$USE_AZURE_STORAGE_FOR_MERGE_TREE" == "1" ]]; then
    # But we still need default disk because some tables loaded only into it
    sudo cat /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml \
      | sed "s|<main><disk>azure</disk></main>|<main><disk>azure</disk></main><default><disk>default</disk></default>|" \
      > /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml.tmp
    mv /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml.tmp /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml
    sudo chown clickhouse /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml
    sudo chgrp clickhouse /etc/clickhouse-server/config.d/azure_storage_policy_by_default.xml
fi


sudo cat /etc/clickhouse-server/config.d/logger_trace.xml \
   | sed "s|<level>trace</level>|<level>test</level>|" \
   > /etc/clickhouse-server/config.d/logger_trace.xml.tmp
mv /etc/clickhouse-server/config.d/logger_trace.xml.tmp /etc/clickhouse-server/config.d/logger_trace.xml

if [ "$cache_policy" = "SLRU" ]; then
    sudo cat /etc/clickhouse-server/config.d/storage_conf.xml \
    | sed "s|<cache_policy>LRU</cache_policy>|<cache_policy>SLRU</cache_policy>|" \
    > /etc/clickhouse-server/config.d/storage_conf.xml.tmp
    mv /etc/clickhouse-server/config.d/storage_conf.xml.tmp /etc/clickhouse-server/config.d/storage_conf.xml
fi

# Randomize async_load_databases
if [ $(( $(date +%-d) % 2 )) -eq 1 ]; then
    sudo echo "<clickhouse><async_load_databases>true</async_load_databases></clickhouse>" \
        > /etc/clickhouse-server/config.d/enable_async_load_databases.xml
fi

start_server

cd /repo/tests/ || exit 1  # clickhouse-test can find queries dir from there
python3 /repo/tests/ci/stress.py --hung-check --drop-databases --output-folder /test_output --skip-func-tests "$SKIP_TESTS_OPTION" --global-time-limit 1200 \
    && echo -e "Test script exit code$OK" >> /test_output/test_results.tsv \
    || echo -e "Test script failed$FAIL script exit code: $?" >> /test_output/test_results.tsv

# NOTE Hung check is implemented in docker/tests/stress/stress
rg -Fa "No queries hung" /test_output/test_results.tsv | grep -Fa "OK" \
  || echo -e "Hung check failed, possible deadlock found (see hung_check.log)$FAIL$(head_escaped /test_output/hung_check.log)" >> /test_output/test_results.tsv

stop_server
mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.stress.log

# NOTE Disable thread fuzzer before server start with data after stress test.
# In debug build it can take a lot of time.
unset "${!THREAD_@}"

start_server

check_server_start

stop_server

[ -f /var/log/clickhouse-server/clickhouse-server.log ] || echo -e "Server log does not exist\tFAIL"
[ -f /var/log/clickhouse-server/stderr.log ] || echo -e "Stderr log does not exist\tFAIL"

mv /var/log/clickhouse-server/clickhouse-server.log /var/log/clickhouse-server/clickhouse-server.final.log

# Grep logs for sanitizer asserts, crashes and other critical errors
check_logs_for_critical_errors

tar -chf /test_output/coordination.tar /var/lib/clickhouse/coordination ||:

collect_query_and_trace_logs

mv /var/log/clickhouse-server/stderr.log /test_output/

# Write check result into check_status.tsv
# Try to choose most specific error for the whole check status
clickhouse-local --structure "test String, res String, time Nullable(Float32), desc String" -q "SELECT 'failure', test FROM table WHERE res != 'OK' order by
(test like '%Sanitizer%') DESC,
(test like '%Killed by signal%') DESC,
(test like '%gdb.log%') DESC,
(test ilike '%possible deadlock%') DESC,
(test like '%start%') DESC,
(test like '%dmesg%') DESC,
(test like '%OOM%') DESC,
(test like '%Signal 9%') DESC,
(test like '%Fatal message%') DESC,
rowNumberInAllBlocks()
LIMIT 1" < /test_output/test_results.tsv > /test_output/check_status.tsv || echo -e "failure\tCannot parse test_results.tsv" > /test_output/check_status.tsv
[ -s /test_output/check_status.tsv ] || echo -e "success\tNo errors found" > /test_output/check_status.tsv

# But OOMs in stress test are allowed
if rg 'OOM in dmesg|Signal 9' /test_output/check_status.tsv
then
    sed -i 's/failure/success/' /test_output/check_status.tsv
fi

collect_core_dumps
