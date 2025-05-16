import os
import subprocess
import time
from pathlib import Path

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


LOG_EXPORT_CONFIG_TEMPLATE = """
remote_servers:
    {CLICKHOUSE_CI_LOGS_CLUSTER}:
        shard:
            replica:
                secure: 1
                user: '{CLICKHOUSE_CI_LOGS_USER}'
                host: '{CLICKHOUSE_CI_LOGS_HOST}'
                port: 9440
                password: '{CLICKHOUSE_CI_LOGS_PASSWORD}'
"""
CLICKHOUSE_CI_LOGS_CLUSTER = "system_logs_export"
CLICKHOUSE_CI_LOGS_USER = "ci"


class ClickHouseProc:
    BACKUPS_XML = """
<clickhouse>
    <backups>
        <type>local</type>
        <path>{CH_RUNTIME_DIR}/var/lib/clickhouse/disks/backups/</path>
    </backups>
</clickhouse>
"""

    def __init__(self, fast_test=False):
        self.ch_config_dir = f"{temp_dir}/etc/clickhouse-server"
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.config_file = f"{self.ch_config_dir}/config.xml"
        self.user_files_path = f"{self.ch_config_dir}/user_files"
        self.test_output_file = f"{temp_dir}/test_result.txt"
        self.command = f"clickhouse-server --config-file {self.config_file} --pid-file {self.pid_file} -- --path {self.ch_config_dir} --user_files_path {self.user_files_path} --top_level_domains_path {self.ch_config_dir}/top_level_domains --keeper_server.storage_path {self.ch_config_dir}/coordination"

        self.ch_config_dir_replica_1 = f"{temp_dir}/etc/clickhouse-server1"
        self.config_file_replica_1 = f"{self.ch_config_dir_replica_1}/config.xml"
        self.ch_config_dir_replica_2 = f"{temp_dir}/etc/clickhouse-server2"
        self.config_file_replica_2 = f"{self.ch_config_dir_replica_2}/config.xml"
        self.pid_file_replica_1 = (
            f"{self.ch_config_dir_replica_1}/clickhouse-server.pid"
        )
        self.pid_file_replica_2 = (
            f"{self.ch_config_dir_replica_2}/clickhouse-server.pid"
        )
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.replica_command_1 = f"clickhouse-server --config-file {self.config_file_replica_1} --daemon --pid-file {self.pid_file_replica_1} -- --path {self.ch_config_dir_replica_1} --logger.stderr {temp_dir}/var/log/clickhouse-server/stderr1.log --logger.log {temp_dir}/var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog {temp_dir}/var/log/clickhouse-server/clickhouse-server1.err.log --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 --mysql_port 19004 --postgresql_port 19005 --keeper_server.tcp_port 19181 --keeper_server.server_id 2 --prometheus.port 19988 --macros.replica r2"
        self.replica_command_2 = f"clickhouse-server --config-file {self.config_file_replica_2} --daemon --pid-file {self.pid_file_replica_2} -- --path {self.ch_config_dir_replica_2} --logger.stderr {temp_dir}/var/log/clickhouse-server/stderr2.log --logger.log {temp_dir}/var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog {temp_dir}/var/log/clickhouse-server/clickhouse-server2.err.log --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 --mysql_port 29004 --postgresql_port 29005 --keeper_server.tcp_port 29181 --keeper_server.server_id 3 --prometheus.port 29988 --macros.shard s2"
        self.proc = None
        self.proc_1 = None
        self.proc_2 = None
        self.pid = 0
        nproc = int(Utils.cpu_count() / 2)
        self.fast_test_command = f"clickhouse-test --hung-check --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --check-zookeeper-session --order random --report-logs-stats --fast-tests-only --no-stateful --jobs {nproc} -- '{{TEST}}' | ts '%Y-%m-%d %H:%M:%S' \
        | tee -a \"{self.test_output_file}\""
        # TODO: store info in case of failure
        self.info = ""
        self.info_file = ""

        Utils.set_env("CLICKHOUSE_CONFIG_DIR", self.ch_config_dir)
        Utils.set_env("CLICKHOUSE_CONFIG", self.config_file)
        Utils.set_env("CLICKHOUSE_USER_FILES", self.user_files_path)
        # Utils.set_env("CLICKHOUSE_SCHEMA_FILES", f"{self.ch_config_dir}/format_schemas")

        # if not fast_test:
        #     with open(f"{self.ch_config_dir}/config.d/backups.xml", "w") as file:
        #         file.write(self.BACKUPS_XML)

        self.minio_proc = None

    def start_minio(self, test_type, log_file_path):
        os.environ["TEMP_DIR"] = f"{Utils.cwd()}/ci/tmp"
        command = [
            "./ci/jobs/scripts/functional_tests/setup_minio.sh",
            test_type,
            "./tests",
        ]
        with open(log_file_path, "w") as log_file:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT
            )
        print(f"Started setup_minio.sh asynchronously with PID {process.pid}")
        return True

    def start_azurite(self, log_file_path):
        command = (
            "azurite-blob --blobHost 0.0.0.0 --blobPort 10000 --silent --inMemoryPersistence",
        )
        with open(log_file_path, "w") as log_file:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT, shell=True
            )
        print(f"Started azurite asynchronously with PID {process.pid}")
        return True

    @staticmethod
    def log_cluster_config():
        return Shell.check(
            f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --config-logs-export-cluster ./tmp_ci/etc/clickhouse-server/config.d/system_logs_export.yaml",
            verbose=True,
        )

    @staticmethod
    def enable_thread_fuzzer_config():
        # For flaky check we also enable thread fuzzer
        os.environ["IS_FLAKY_CHECK"] = "1"
        os.environ["THREAD_FUZZER_CPU_TIME_PERIOD_US"] = "1000"
        os.environ["THREAD_FUZZER_SLEEP_PROBABILITY"] = "0.1"
        os.environ["THREAD_FUZZER_SLEEP_TIME_US_MAX"] = "100000"

        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY"] = "1"
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY"] = "1"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY"] = (
            "1"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY"] = "1"

        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY"] = (
            "0.001"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY"] = "0.001"

        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY"] = (
            "0.001"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY"] = (
            "0.001"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US_MAX"] = (
            "10000"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US_MAX"] = "10000"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US_MAX"] = (
            "10000"
        )
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US_MAX"] = (
            "10000"
        )

    def create_log_export_config(self):
        print("Create log export config")
        config_file = Path(self.ch_config_dir) / "config.d" / "system_logs_export.yaml"

        self.log_export_host = Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        self.log_export_password = Secret.Config(
            name="clickhouse_ci_logs_password",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        config_content = LOG_EXPORT_CONFIG_TEMPLATE.format(
            CLICKHOUSE_CI_LOGS_CLUSTER=CLICKHOUSE_CI_LOGS_CLUSTER,
            CLICKHOUSE_CI_LOGS_HOST=self.log_export_host,
            CLICKHOUSE_CI_LOGS_USER=CLICKHOUSE_CI_LOGS_USER,
            CLICKHOUSE_CI_LOGS_PASSWORD=self.log_export_password,
        )

        with open(config_file, "w") as f:
            f.write(config_content)

    def start_log_exports(self, check_start_time):
        print("Start log export")
        os.environ["CLICKHOUSE_CI_LOGS_CLUSTER"] = CLICKHOUSE_CI_LOGS_CLUSTER
        os.environ["CLICKHOUSE_CI_LOGS_HOST"] = self.log_export_host
        os.environ["CLICKHOUSE_CI_LOGS_USER"] = CLICKHOUSE_CI_LOGS_USER
        os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = self.log_export_password
        info = Info()
        os.environ["EXTRA_COLUMNS_EXPRESSION"] = (
            f"CAST({info.pr_number} AS UInt32) AS pull_request_number, '{info.sha}' AS commit_sha, toDateTime('{Utils.timestamp_to_str(check_start_time)}', 'UTC') AS check_start_time, toLowCardinality('{info.job_name}') AS check_name, toLowCardinality('{info.instance_type}') AS instance_type, '{info.instance_id}' AS instance_id"
        )

        return Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication",
            verbose=True,
        )

    @staticmethod
    def stop_log_exports():
        return Shell.check(
            f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
            verbose=True,
        )

    def start(self, replicated=False):
        print("Starting ClickHouse server")
        Shell.check(f"rm {self.pid_file}")
        self.proc = subprocess.Popen(self.command, stderr=subprocess.STDOUT, shell=True)
        started = False
        try:
            for _ in range(5):
                pid = Shell.get_output(f"cat {self.pid_file}").strip()
                if not pid:
                    Utils.sleep(1)
                    continue
                started = True
                print(f"Got pid from fs [{pid}]")
                _ = int(pid)
                break
        except Exception:
            pass

        if not started:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False

        print(f"ClickHouse server started successfully, pid [{pid}]")

        if replicated:
            return self.start_replica(1) and self.start_replica(2)
        return True

    def start_replica(self, replica_num):
        print(f"Starting ClickHouse server replica {replica_num}")
        if replica_num == 1:
            pid_file = self.pid_file_replica_1
            command = self.replica_command_1
        elif replica_num == 2:
            pid_file = self.pid_file_replica_2
            command = self.replica_command_2
        else:
            assert False

        Shell.check(f"rm {pid_file}")

        proc = subprocess.Popen(command, stderr=subprocess.STDOUT, shell=True)
        if replica_num == 1:
            self.proc_1 = proc
        elif replica_num == 2:
            self.proc_2 = proc
        started = False
        try:
            for _ in range(5):
                pid = Shell.get_output(f"cat {pid_file}").strip()
                if not pid:
                    Utils.sleep(1)
                    continue
                started = True
                print(f"Got pid from fs [{pid}]")
                _ = int(pid)
                break
        except Exception:
            pass

        if not started:
            stdout = proc.stdout.read().strip() if proc.stdout else ""
            stderr = proc.stderr.read().strip() if proc.stderr else ""
            Utils.print_formatted_error(
                f"Failed to start ClickHouse replica {replica_num}", stdout, stderr
            )
            return False

        print(f"ClickHouse server started successfully, pid [{pid}]")
        return True

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                'clickhouse-client --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True

    def flush_system_logs(self):
        for proc, port in zip(
            (self.proc, self.proc_1, self.proc_2), (9000, 19000, 29000)
        ):
            if proc:
                res = Shell.check(
                    f'clickhouse-client --port {port} --query "system flush logs"',
                    verbose=True,
                )
                if not res:
                    return False
        return True

    def prepare_stateful_data(self, with_s3_storage, is_db_replicated):
        if is_db_replicated:
            print("Skip stateful data preparation for db replicated")
            return True
        command = """
set -e
set -o pipefail

clickhouse-client --query "SHOW DATABASES"
clickhouse-client --query "CREATE DATABASE datasets"
clickhouse-client < ./tests/docker_scripts/create.sql
clickhouse-client --query "SHOW TABLES FROM datasets"

clickhouse-client --query "CREATE DATABASE test"
clickhouse-client --query "SHOW TABLES FROM test"
if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
    clickhouse-client --query "CREATE TABLE test.hits (WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16, EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32, UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String, RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8, MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16, ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32, SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8, FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32, HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String, HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32, FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32, LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String, OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String, UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64, URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String, ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64), IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8)
        ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate)
        ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
    clickhouse-client --query "CREATE TABLE test.visits (CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8, VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32, Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String, EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String, AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32), RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32, SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32, ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32, SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16, UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16, FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8, FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8, Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8, BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), Params Array(String),  Goals Nested(ID UInt32, Serial UInt32, EventTime DateTime,  Price Int64,  OrderID String, CurrencyID UInt32), WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64, ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32, ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32, ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32, ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16, ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32, OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime, PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  TraficSource    Nested(ID Int8,  SearchEngineID UInt16, AdvEngineID UInt8, PlaceID UInt16, SocialSourceNetworkID UInt8, Domain String, SearchPhrase String, SocialSourcePage String),  Attendance FixedString(16), CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64, StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64, OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64, UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32, ParsedParams    Nested(Key1 String,  Key2 String,  Key3 String,  Key4 String, Key5 String, ValueDouble    Float64), Market Nested(Type UInt8, GoalID UInt32, OrderID String,  OrderPrice Int64,  PP UInt32,  DirectPlaceID UInt32,  DirectOrderID  UInt32, DirectBannerID UInt32,  GoodID String, GoodName String, GoodQuantity Int32,  GoodPrice Int64),  IslandID FixedString(16))
        ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
        SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"

    clickhouse-client --max_execution_time 600 --max_memory_usage 25G --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
    clickhouse-client --max_execution_time 600 --max_memory_usage 25G --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
    clickhouse-client --query "DROP TABLE datasets.visits_v1 SYNC"
    clickhouse-client --query "DROP TABLE datasets.hits_v1 SYNC"
else
    clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
    clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
fi
clickhouse-client --query "CREATE TABLE test.hits_s3  (WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
# AWS S3 is very inefficient, so increase memory even further:
clickhouse-client --max_execution_time 600 --max_memory_usage 30G --max_memory_usage_for_user 30G --query "INSERT INTO test.hits_s3 SELECT * FROM test.hits SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"

clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "SELECT count() FROM test.hits"
clickhouse-client --query "SELECT count() FROM test.visits"
"""
        if with_s3_storage:
            command = "USE_S3_STORAGE_FOR_MERGE_TREE=1\n" + command
        return Shell.check(command, strict=True)

    def insert_system_zookeeper_config(self):
        for _ in range(10):
            res = Shell.check(
                f"clickhouse-client --query \"insert into system.zookeeper (name, path, value) values ('auxiliary_zookeeper2', '/test/chroot/', '')\"",
                verbose=True,
            )
            time.sleep(1)
            if not res:
                return True
        else:
            return False

    def run_fast_test(self, test=""):
        if Path(self.test_output_file).exists():
            Path(self.test_output_file).unlink()
        exit_code = Shell.run(self.fast_test_command.format(TEST=test), verbose=True)
        return exit_code == 0

    def terminate(self):
        print("Terminate ClickHouse processes")
        timeout = 10
        for proc in (self.proc_2, self.proc_1, self.proc):
            if proc:
                Utils.terminate_process_group(proc.pid)

                proc.terminate()
                try:
                    proc.wait(timeout=10)
                    print(f"Process {proc.pid} terminated gracefully.")
                except Exception:
                    print(
                        f"Process {proc.pid} did not terminate in {timeout} seconds, killing it..."
                    )
                    Utils.terminate_process_group(proc.pid, force=True)
                    proc.wait()  # Wait for the process to be fully killed
                    print(f"Process {proc} was killed.")

        if self.minio_proc:
            Utils.terminate_process_group(self.minio_proc.pid)


class ClickHouseLight:
    def __init__(self):
        self.path = temp_dir
        self.config_path = f"{temp_dir}/config"
        self.pid_file = "/tmp/pid"
        self.start_cmd = f"{self.path}/clickhouse-server --config-file={self.config_path}/config.xml --pid-file {self.pid_file}"
        self.log_file = f"{temp_dir}/server.log"
        self.port = 9000
        self.pid = None

    def install(self):
        Utils.add_to_PATH(self.path)
        commands = [
            f"mkdir -p {self.config_path}/users.d",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {self.config_path}",
            # make it ipv4 only
            f'sed -i "s|<!-- <listen_host>0.0.0.0</listen_host> -->|<listen_host>0.0.0.0</listen_host>|" {self.config_path}/config.xml',
            f"cp -r --dereference ./programs/server/config.d {self.config_path}",
            f"chmod +x {self.path}/clickhouse",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-server",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-client",
        ]
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def clickbench_config_tweaks(self):
        content = """
profiles:
    default:
        allow_introspection_functions: 1
"""
        file_path = f"{self.config_path}/users.d/allow_introspection_functions.yaml"
        with open(file_path, "w") as file:
            file.write(content)
        return True

    def fuzzer_config_tweaks(self):
        # TODO figure out which ones are needed
        commands = [
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml {self.config_path}/users.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/allow-nullable-key.xml {self.config_path}/config.d",
        ]

        c1 = """
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.75</max_server_memory_usage_to_ram_ratio>
</clickhouse>
"""
        c2 = """
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
"""
        file_path = (
            f"{self.config_path}/config.d/max_server_memory_usage_to_ram_ratio.xml"
        )
        with open(file_path, "w") as file:
            file.write(c1)

        file_path = f"{self.config_path}/config.d/core.xml"
        with open(file_path, "w") as file:
            file.write(c2)
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def create_log_export_config(self):
        print("Create log export config")
        config_file = Path(self.config_path) / "config.d" / "system_logs_export.yaml"

        self.log_export_host = Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        self.log_export_password = Secret.Config(
            name="clickhouse_ci_logs_password",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        config_content = LOG_EXPORT_CONFIG_TEMPLATE.format(
            CLICKHOUSE_CI_LOGS_CLUSTER=CLICKHOUSE_CI_LOGS_CLUSTER,
            CLICKHOUSE_CI_LOGS_HOST=self.log_export_host,
            CLICKHOUSE_CI_LOGS_USER=CLICKHOUSE_CI_LOGS_USER,
            CLICKHOUSE_CI_LOGS_PASSWORD=self.log_export_password,
        )

        with open(config_file, "w") as f:
            f.write(config_content)

    def start_log_exports(self, check_start_time):
        print("Start log export")
        os.environ["CLICKHOUSE_CI_LOGS_CLUSTER"] = CLICKHOUSE_CI_LOGS_CLUSTER
        os.environ["CLICKHOUSE_CI_LOGS_HOST"] = self.log_export_host
        os.environ["CLICKHOUSE_CI_LOGS_USER"] = CLICKHOUSE_CI_LOGS_USER
        os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = self.log_export_password
        info = Info()
        os.environ["EXTRA_COLUMNS_EXPRESSION"] = (
            f"CAST({info.pr_number} AS UInt32) AS pull_request_number, '{info.sha}' AS commit_sha, toDateTime('{Utils.timestamp_to_str(check_start_time)}', 'UTC') AS check_start_time, toLowCardinality('{info.job_name}') AS check_name, toLowCardinality('{info.instance_type}') AS instance_type, '{info.instance_id}' AS instance_id"
        )

        Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication",
            verbose=True,
            strict=True,
        )

    def start(self):
        print(f"Starting ClickHouse server")
        print("Command: ", self.start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.start_cmd, stderr=subprocess.STDOUT, stdout=self.log_fd, shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        self.pid = int(Shell.get_output(f"cat {self.pid_file}").strip())
        return True

    def attach_gdb(self):
        assert self.pid, "ClickHouse not started"
        rtmin = Shell.get_output("kill -l SIGRTMIN", strict=True)
        script = f"""
    set follow-fork-mode parent
    handle SIGHUP nostop noprint pass
    handle SIGINT nostop noprint pass
    handle SIGQUIT nostop noprint pass
    handle SIGPIPE nostop noprint pass
    handle SIGTERM nostop noprint pass
    handle SIGUSR1 nostop noprint pass
    handle SIGUSR2 nostop noprint pass
    handle SIG{rtmin} nostop noprint pass
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
    p "done"
    detach
    quit
"""
        script_path = f"./script.gdb"
        with open(script_path, "w") as file:
            file.write(script)

        self.proc = subprocess.Popen(
            f"gdb -batch -command {script_path} -p {self.pid}", shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to attaach gdb", stdout, stderr)
            return False

        # gdb will send SIGSTOP, spend some time loading debug info, and then send SIGCONT, wait for it (up to send_timeout, 300s)
        Shell.check(
            "time clickhouse-client --query \"SELECT 'Connected to clickhouse-server after attaching gdb'\"",
            verbose=True,
        )

        # Check connectivity after we attach gdb, because it might cause the server
        # to freeze, and the fuzzer will fail. In debug build, it can take a lot of time.
        res = self.wait_ready()
        if res:
            print("GDB attached successfully")
        return res
