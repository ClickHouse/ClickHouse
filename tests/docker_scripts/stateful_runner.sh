#!/bin/bash

# shellcheck disable=SC1091
source /setup_export_logs.sh
set -e -x

# Choose random timezone for this test run
TZ="$(rg -v '#' /usr/share/zoneinfo/zone.tab | awk '{print $3}' | shuf | head -n1)"
echo "Choosen random timezone $TZ"
ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

dpkg -i package_folder/clickhouse-common-static_*.deb;
dpkg -i package_folder/clickhouse-common-static-dbg_*.deb
dpkg -i package_folder/clickhouse-server_*.deb
dpkg -i package_folder/clickhouse-client_*.deb

ln -s /repo/tests/clickhouse-test /usr/bin/clickhouse-test

# shellcheck disable=SC1091
source /repo/tests/docker_scripts/utils.lib

# install test configs
/repo/tests/config/install.sh

azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --silent --inMemoryPersistence &

/repo/tests/docker_scripts/setup_minio.sh stateful
./mc admin trace clickminio > /test_output/minio.log &
MC_ADMIN_PID=$!

config_logs_export_cluster /etc/clickhouse-server/config.d/system_logs_export.yaml

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

if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
    # It is not needed, we will explicitly create tables on s3.
    # We do not have statefull tests with s3 storage run in public repository, but this is needed for another repository.
    rm /etc/clickhouse-server/config.d/s3_storage_policy_for_merge_tree_by_default.xml

    rm /etc/clickhouse-server/config.d/storage_metadata_with_full_object_key.xml
    rm /etc/clickhouse-server/config.d/s3_storage_policy_with_template_object_key.xml
fi

function start()
{
    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        mkdir -p /var/run/clickhouse-server1
        sudo chown clickhouse:clickhouse /var/run/clickhouse-server1
        # NOTE We run "clickhouse server" instead of "clickhouse-server"
        # to make "pidof clickhouse-server" return single pid of the main instance.
        # We wil run main instance using "service clickhouse-server start"
        sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server1/config.xml --daemon \
        --pid-file /var/run/clickhouse-server1/clickhouse-server.pid \
        -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
        --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
        --mysql_port 19004 --postgresql_port 19005 \
        --keeper_server.tcp_port 19181 --keeper_server.server_id 2

        mkdir -p /var/run/clickhouse-server2
        sudo chown clickhouse:clickhouse /var/run/clickhouse-server2
        sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
        --pid-file /var/run/clickhouse-server2/clickhouse-server.pid \
        -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
        --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
        --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \
        --mysql_port 29004 --postgresql_port 29005 \
        --keeper_server.tcp_port 29181 --keeper_server.server_id 3
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
        timeout 120 sudo -E -u clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml --daemon --pid-file /var/run/clickhouse-server/clickhouse-server.pid
        sleep 0.5
        counter=$((counter + 1))
    done
}

start

setup_logs_replication

clickhouse-client --query "SHOW DATABASES"
clickhouse-client --query "CREATE DATABASE datasets"
clickhouse-client < /repo/tests/docker_scripts/create.sql
clickhouse-client --query "SHOW TABLES FROM datasets"

if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    clickhouse-client --query "CREATE DATABASE test ON CLUSTER 'test_cluster_database_replicated'
        ENGINE=Replicated('/test/clickhouse/db/test', '{shard}', '{replica}')"

    clickhouse-client --query "CREATE TABLE test.hits AS datasets.hits_v1"
    clickhouse-client --query "CREATE TABLE test.visits AS datasets.visits_v1"

    clickhouse-client --max_memory_usage 10G --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1"
    clickhouse-client --max_memory_usage 10G --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1"

    clickhouse-client --query "DROP TABLE datasets.hits_v1"
    clickhouse-client --query "DROP TABLE datasets.visits_v1"
else
    clickhouse-client --query "CREATE DATABASE test"
    clickhouse-client --query "SHOW TABLES FROM test"
    if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
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

        clickhouse-client --max_memory_usage 10G --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
        clickhouse-client --max_memory_usage 10G --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
        clickhouse-client --query "DROP TABLE datasets.visits_v1 SYNC"
        clickhouse-client --query "DROP TABLE datasets.hits_v1 SYNC"
    else
        clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
        clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
    fi
    clickhouse-client --query "CREATE TABLE test.hits_s3  (WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
    # AWS S3 is very inefficient, so increase memory even further:
    clickhouse-client --max_memory_usage 30G --max_memory_usage_for_user 30G --query "INSERT INTO test.hits_s3 SELECT * FROM test.hits SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
fi

clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "SELECT count() FROM test.hits"
clickhouse-client --query "SELECT count() FROM test.visits"

function run_tests()
{
    set -x
    # We can have several additional options so we path them as array because it's
    # more idiologically correct.
    read -ra ADDITIONAL_OPTIONS <<< "${ADDITIONAL_OPTIONS:-}"

    if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--replicated-database')
    fi

    if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--s3-storage')
    fi

    if [[ -n "$USE_AZURE_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_AZURE_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--azure-blob-storage')
    fi

    if [[ -n "$USE_DATABASE_ORDINARY" ]] && [[ "$USE_DATABASE_ORDINARY" -eq 1 ]]; then
        ADDITIONAL_OPTIONS+=('--db-engine=Ordinary')
    fi

    set +e

    TEST_ARGS=(
        -j 2
        --testname
        --shard
        --zookeeper
        --check-zookeeper-session
        --no-stateless
        --hung-check
        --print-time
        --capture-client-stacktrace
        --queries "/repo/tests/queries"
        "${ADDITIONAL_OPTIONS[@]}"
        "$SKIP_TESTS_OPTION"
    )
    if [[ -n "$USE_PARALLEL_REPLICAS" ]] && [[ "$USE_PARALLEL_REPLICAS" -eq 1 ]]; then
        TEST_ARGS+=(
            --client="clickhouse-client --allow_experimental_parallel_reading_from_replicas=1 --parallel_replicas_for_non_replicated_merge_tree=1 --max_parallel_replicas=100 --cluster_for_parallel_replicas='parallel_replicas'"
            --no-parallel-replicas
        )
    fi
    clickhouse-test "${TEST_ARGS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
    set -e
}

export -f run_tests

run_tests ||:

echo "Files in current directory"
ls -la ./
echo "Files in root directory"
ls -la /

/repo/tests/docker_scripts/process_functional_tests_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv

sudo clickhouse stop ||:
if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    sudo clickhouse stop --pid-path /var/run/clickhouse-server1 ||:
    sudo clickhouse stop --pid-path /var/run/clickhouse-server2 ||:
fi

# Kill minio admin client to stop collecting logs
kill $MC_ADMIN_PID
rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server.log ||:

zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server.log > /test_output/clickhouse-server.log.zst ||:
# FIXME: remove once only github actions will be left
rm /var/log/clickhouse-server/clickhouse-server.log
mv /var/log/clickhouse-server/stderr.log /test_output/ ||:

if [[ -n "$WITH_COVERAGE" ]] && [[ "$WITH_COVERAGE" -eq 1 ]]; then
    tar --zstd -c -h -f /test_output/clickhouse_coverage.tar.zst /profraw ||:
fi
if [[ -n "$USE_DATABASE_REPLICATED" ]] && [[ "$USE_DATABASE_REPLICATED" -eq 1 ]]; then
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server1.log ||:
    rg -Fa "<Fatal>" /var/log/clickhouse-server/clickhouse-server2.log ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server1.log > /test_output/clickhouse-server1.log.zst ||:
    zstd --threads=0 < /var/log/clickhouse-server/clickhouse-server2.log > /test_output/clickhouse-server2.log.zst ||:
    # FIXME: remove once only github actions will be left
    rm /var/log/clickhouse-server/clickhouse-server1.log
    rm /var/log/clickhouse-server/clickhouse-server2.log
    mv /var/log/clickhouse-server/stderr1.log /test_output/ ||:
    mv /var/log/clickhouse-server/stderr2.log /test_output/ ||:
fi

collect_core_dumps
