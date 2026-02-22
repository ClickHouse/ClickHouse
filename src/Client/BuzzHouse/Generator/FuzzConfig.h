#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "config.h"

#if USE_SIMDJSON
#    include <Common/JSONParsers/SimdJSONParser.h>
namespace BuzzHouse
{
using JSONParserImpl = DB::SimdJSONParser;
}
#elif USE_RAPIDJSON
#    include <Common/JSONParsers/RapidJSONParser.h>
namespace BuzzHouse
{
using JSONParserImpl = DB::RapidJSONParser;
}
#else
#    include <Common/JSONParsers/DummyJSONParser.h>
namespace BuzzHouse
{
using JSONParserImpl = DB::DummyJSONParser;
}
#endif

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/ClientBase.h>
#include <Common/logger_useful.h>

namespace BuzzHouse
{

const constexpr uint64_t allow_bool = (UINT64_C(1) << 0), allow_unsigned_int = (UINT64_C(1) << 1), allow_int8 = (UINT64_C(1) << 2),
                         allow_int64 = (UINT64_C(1) << 3), allow_int128 = (UINT64_C(1) << 4), allow_float32 = (UINT64_C(1) << 5),
                         allow_dates = (UINT64_C(1) << 6), allow_date32 = (UINT64_C(1) << 7), allow_datetimes = (UINT64_C(1) << 8),
                         allow_datetime64 = (UINT64_C(1) << 9), allow_strings = (UINT64_C(1) << 10), allow_decimals = (UINT64_C(1) << 11),
                         allow_uuid = (UINT64_C(1) << 12), allow_enum = (UINT64_C(1) << 13), allow_dynamic = (UINT64_C(1) << 14),
                         allow_JSON = (UINT64_C(1) << 15), allow_nullable = (UINT64_C(1) << 16),
                         allow_low_cardinality = (UINT64_C(1) << 17), allow_array = (UINT64_C(1) << 18), allow_map = (UINT64_C(1) << 19),
                         allow_tuple = (UINT64_C(1) << 20), allow_variant = (UINT64_C(1) << 21), allow_nested = (UINT64_C(1) << 22),
                         allow_ipv4 = (UINT64_C(1) << 23), allow_ipv6 = (UINT64_C(1) << 24), allow_geo = (UINT64_C(1) << 25),
                         set_any_datetime_precision = (UINT64_C(1) << 26), set_no_decimal_limit = (UINT64_C(1) << 27),
                         allow_fixed_strings = (UINT64_C(1) << 28), allow_time = (UINT64_C(1) << 29), allow_time64 = (UINT64_C(1) << 30),
                         allow_int16 = (UINT64_C(1) << 31), allow_float64 = (UINT64_C(1) << 32), allow_bfloat16 = (UINT64_C(1) << 33),
                         allow_qbit = (UINT64_C(1) << 34), allow_aggregate = (UINT64_C(1) << 35),
                         allow_simple_aggregate = (UINT64_C(1) << 36);

const constexpr uint64_t allow_replacing_mergetree
    = (UINT64_C(1) << 0),
    allow_coalescing_mergetree = (UINT64_C(1) << 1), allow_summing_mergetree = (UINT64_C(1) << 2),
    allow_aggregating_mergetree = (UINT64_C(1) << 3), allow_collapsing_mergetree = (UINT64_C(1) << 4),
    allow_versioned_collapsing_mergetree = (UINT64_C(1) << 5), allow_file = (UINT64_C(1) << 6), allow_null = (UINT64_C(1) << 7),
    allow_setengine = (UINT64_C(1) << 8), allow_join = (UINT64_C(1) << 9), allow_memory = (UINT64_C(1) << 10),
    allow_stripelog = (UINT64_C(1) << 11), allow_log = (UINT64_C(1) << 12), allow_tinylog = (UINT64_C(1) << 13),
    allow_embedded_rocksdb = (UINT64_C(1) << 14), allow_buffer = (UINT64_C(1) << 15), allow_mysql = (UINT64_C(1) << 16),
    allow_postgresql = (UINT64_C(1) << 17), allow_sqlite = (UINT64_C(1) << 18), allow_mongodb = (UINT64_C(1) << 19),
    allow_redis = (UINT64_C(1) << 20), allow_S3 = (UINT64_C(1) << 21), allow_S3queue = (UINT64_C(1) << 22),
    allow_hudi = (UINT64_C(1) << 23), allow_deltalakeS3 = (UINT64_C(1) << 24), allow_deltalakeAzure = (UINT64_C(1) << 25),
    allow_deltalakelocal = (UINT64_C(1) << 26), allow_icebergS3 = (UINT64_C(1) << 27), allow_icebergAzure = (UINT64_C(1) << 28),
    allow_icebergLocal = (UINT64_C(1) << 29), allow_merge = (UINT64_C(1) << 30), allow_distributed = (UINT64_C(1) << 31),
    allow_dictionary = (UINT64_C(1) << 32), allow_generaterandom = (UINT64_C(1) << 33), allow_AzureBlobStorage = (UINT64_C(1) << 34),
    allow_AzureQueue = (UINT64_C(1) << 35), allow_URL = (UINT64_C(1) << 36), allow_keepermap = (UINT64_C(1) << 37),
    allow_external_distributed = (UINT64_C(1) << 38), allow_materialized_postgresql = (UINT64_C(1) << 39),
    allow_replicated = (UINT64_C(1) << 40), allow_shared = (UINT64_C(1) << 41), allow_datalakecatalog = (UINT64_C(1) << 42),
    allow_arrowflight = (UINT64_C(1) << 43), allow_alias = (UINT64_C(1) << 44), allow_kafka = (UINT64_C(1) << 45);

extern const DB::Strings compressionMethods;

using JSONObjectType = JSONParserImpl::Element;

class Catalog
{
public:
    String client_hostname;
    String server_hostname;
    String path;
    String region;
    String warehouse;
    uint32_t port;

    Catalog()
        : client_hostname("localhost")
        , server_hostname("localhost")
        , path()
        , region()
        , warehouse()
        , port(0)
    {
    }

    Catalog(
        const String & client_hostname_,
        const String & server_hostname_,
        const String & path_,
        const String & region_,
        const String & warehouse_,
        const uint32_t port_)
        : client_hostname(client_hostname_)
        , server_hostname(server_hostname_)
        , path(path_)
        , region(region_)
        , warehouse(warehouse_)
        , port(port_)
    {
    }

    Catalog(const Catalog & c) = default;
    Catalog(Catalog && c) = default;
    Catalog & operator=(const Catalog & c) = default;
    Catalog & operator=(Catalog && c) noexcept = default;
};

class ServerCredentials
{
public:
    String client_hostname;
    String server_hostname;
    String container;
    uint32_t port;
    uint32_t mysql_port;
    String unix_socket;
    String user;
    String password;
    String secret;
    String database;
    String named_collection;
    std::filesystem::path user_files_dir;
    std::filesystem::path query_log_file;
    std::optional<Catalog> glue_catalog;
    std::optional<Catalog> hive_catalog;
    std::optional<Catalog> rest_catalog;
    std::optional<Catalog> unity_catalog;

    ServerCredentials()
        : client_hostname("localhost")
        , server_hostname("localhost")
        , port(0)
        , mysql_port(0)
        , user("test")
    {
    }

    ServerCredentials(
        const String & client_hostname_,
        const String & server_hostname_,
        const String & container_,
        const uint32_t port_,
        const uint32_t mysql_port_,
        const String & unix_socket_,
        const String & user_,
        const String & password_,
        const String & secret_,
        const String & database_,
        const String & named_collection_,
        const std::filesystem::path & user_files_dir_,
        const std::filesystem::path & query_log_file_,
        const std::optional<Catalog> glue_catalog_,
        const std::optional<Catalog> hive_catalog_,
        const std::optional<Catalog> rest_catalog_,
        const std::optional<Catalog> unity_catalog_)
        : client_hostname(client_hostname_)
        , server_hostname(server_hostname_)
        , container(container_)
        , port(port_)
        , mysql_port(mysql_port_)
        , unix_socket(unix_socket_)
        , user(user_)
        , password(password_)
        , secret(secret_)
        , database(database_)
        , named_collection(named_collection_)
        , user_files_dir(user_files_dir_)
        , query_log_file(query_log_file_)
        , glue_catalog(glue_catalog_)
        , hive_catalog(hive_catalog_)
        , rest_catalog(rest_catalog_)
        , unity_catalog(unity_catalog_)
    {
    }

    ServerCredentials(const ServerCredentials & sc) = default;
    ServerCredentials(ServerCredentials && sc) = default;
    ServerCredentials & operator=(const ServerCredentials & sc) = default;
    ServerCredentials & operator=(ServerCredentials && sc) noexcept = default;
};

class PerformanceMetric
{
public:
    bool enabled = false;
    uint64_t threshold = 10;
    uint64_t minimum = 1000;

    PerformanceMetric() = default;

    PerformanceMetric(const bool enabled_, const uint64_t threshold_, const uint64_t minimum_)
        : enabled(enabled_)
        , threshold(threshold_)
        , minimum(minimum_)
    {
    }

    PerformanceMetric(const PerformanceMetric & pm) = default;
    PerformanceMetric(PerformanceMetric && pm) = default;
    PerformanceMetric & operator=(const PerformanceMetric & pm) = default;
    PerformanceMetric & operator=(PerformanceMetric && pm) noexcept = default;
};

class PerformanceResult
{
public:
    /// The metrics and respective value
    std::unordered_map<String, uint64_t> metrics;
    /// The metrics and respective String representation (I can improve this)
    std::unordered_map<String, String> result_strings;

    PerformanceResult() = default;

    PerformanceResult(const PerformanceResult & pr) = default;
    PerformanceResult(PerformanceResult && pr) = default;
    PerformanceResult & operator=(const PerformanceResult & pr) = default;
    PerformanceResult & operator=(PerformanceResult && pr) noexcept = default;
};

class SystemTable
{
public:
    String schema_name;
    String table_name;
    DB::Strings columns;

    SystemTable()
        : schema_name("system")
        , table_name("tables")
        , columns()
    {
    }

    SystemTable(const String & schema_name_, const String & table_name_, const DB::Strings & columns_)
        : schema_name(schema_name_)
        , table_name(table_name_)
        , columns(columns_)
    {
    }

    SystemTable(const SystemTable & c) = default;
    SystemTable(SystemTable && c) = default;
    SystemTable & operator=(const SystemTable & c) = default;
    SystemTable & operator=(SystemTable && c) noexcept = default;

    void setName(ExprSchemaTable * est) const
    {
        est->mutable_database()->set_database(schema_name);
        est->mutable_table()->set_table(table_name);
    }
};

class FuzzConfig
{
private:
    DB::ClientBase * cb = nullptr;

public:
    LoggerPtr log;
    std::ofstream outf;
    DB::Strings collations;
    DB::Strings storage_policies;
    DB::Strings timezones;
    DB::Strings disks;
    DB::Strings keeper_disks;
    DB::Strings clusters;
    DB::Strings caches;
    DB::Strings failpoints;
    DB::Strings remote_servers;
    DB::Strings remote_secure_servers;
    DB::Strings http_servers;
    DB::Strings https_servers;
    DB::Strings arrow_flight_servers;
    DB::Strings hot_settings;
    DB::Strings disallowed_settings;
    DB::Strings hot_table_settings;

    std::optional<ServerCredentials> clickhouse_server;
    std::optional<ServerCredentials> mysql_server;
    std::optional<ServerCredentials> postgresql_server;
    std::optional<ServerCredentials> sqlite_server;
    std::optional<ServerCredentials> mongodb_server;
    std::optional<ServerCredentials> redis_server;
    std::optional<ServerCredentials> minio_server;
    std::optional<ServerCredentials> http_server;
    std::optional<ServerCredentials> azurite_server;
    std::optional<ServerCredentials> kafka_server;
    std::optional<ServerCredentials> dolor_server;

    std::unordered_map<String, PerformanceMetric> metrics;

    std::unordered_set<uint32_t> disallowed_error_codes;
    std::unordered_set<uint32_t> oracle_ignore_error_codes;

    String host = "localhost";
    String keeper_map_path_prefix;

    bool read_log = false;
    bool fuzz_floating_points = true;
    bool test_with_fill = true;
    bool compare_success_results = false;
    bool measure_performance = false;
    bool allow_infinite_tables = false;
    bool compare_explains = false;
    bool allow_memory_tables = true;
    bool allow_client_restarts = false;
    bool enable_fault_injection_settings = false;
    bool enable_force_settings = false;
    bool allow_hardcoded_inserts = true;
    bool allow_async_requests = false;
    bool truncate_output = false;
    bool allow_transactions = true;
    bool enable_overflow_settings = false;
    bool random_limited_values = false;
    bool set_smt_disk = true;
    bool allow_query_oracles = true;
    bool allow_health_check = true;
    bool enable_compatibility_settings = false;
    bool enable_memory_settings = false;
    bool enable_backups = true;
    bool enable_renames = true;

    uint64_t seed = 0;
    uint64_t min_insert_rows = 1;
    uint64_t max_insert_rows = 1000;
    uint64_t min_nested_rows = 0;
    uint64_t max_nested_rows = 10;
    uint64_t flush_log_wait_time = 1000;
    uint64_t type_mask = std::numeric_limits<uint64_t>::max();
    uint64_t engine_mask = std::numeric_limits<uint64_t>::max();

    uint32_t max_depth = 3;
    uint32_t max_width = 3;
    uint32_t max_databases = 4;
    uint32_t max_functions = 4;
    uint32_t max_tables = 10;
    uint32_t max_views = 5;
    uint32_t max_dictionaries = 5;
    uint32_t max_columns = 5;
    uint32_t time_to_run = 0;
    uint32_t port = 9000;
    uint32_t secure_port = 9440;
    uint32_t http_port = 8123;
    uint32_t http_secure_port = 8443;
    uint32_t use_dump_table_oracle = 2;
    uint32_t max_reconnection_attempts = 3;
    uint32_t time_to_sleep_between_reconnects = 3000;
    uint32_t min_string_length = 0;
    uint32_t max_string_length = 1009;
    uint32_t max_parallel_queries = 5;
    uint32_t max_number_alters = 4;
    uint32_t deterministic_prob = 50;

    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql";
    std::filesystem::path client_file_path = "/var/lib/clickhouse/user_files";
    std::filesystem::path server_file_path = "/var/lib/clickhouse/user_files";
    std::filesystem::path fuzz_client_out = client_file_path / "fuzz.data";
    std::filesystem::path fuzz_server_out = server_file_path / "fuzz.data";
    std::filesystem::path lakes_path = "/var/lib/clickhouse/user_files/lakehouses";

    FuzzConfig()
        : cb(nullptr)
        , log(getLogger("BuzzHouse"))
    {
    }

    FuzzConfig(DB::ClientBase * c, const String & path);

    bool processServerQuery(bool outlog, const String & query);

private:
    template <typename T>
    void loadServerSettings(std::vector<T> & out, const String & desc, const String & query);

public:
    void loadServerConfigurations();

    String getConnectionHostAndPort(bool secure) const;

    String getHTTPURL(bool secure) const;

    void loadSystemTables(std::vector<SystemTable> & tables);

    bool hasMutations();

    String getRandomMutation(uint64_t rand_val);

    String getRandomIcebergHistoryValue(const String & property);

    String getRandomFileSystemCacheValue();

    bool tableHasPartitions(bool detached, const String & database, const String & table);

    String tableGetRandomPartitionOrPart(uint64_t rand_val, bool detached, bool partition, const String & database, const String & table);

    void comparePerformanceResults(const String & oracle_name, PerformanceResult & server, PerformanceResult & peer) const;

    void validateClickHouseHealth();
};

}
