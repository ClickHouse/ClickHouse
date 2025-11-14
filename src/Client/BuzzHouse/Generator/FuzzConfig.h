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
    allow_arrowflight = (UINT64_C(1) << 43), allow_alias = (UINT64_C(1) << 44);

extern const DB::Strings compressionMethods;

using JSONObjectType = JSONParserImpl::Element;

class Catalog
{
public:
    String client_hostname, server_hostname, path, region, warehouse;
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
    String client_hostname, server_hostname, container;
    uint32_t port, mysql_port;
    String unix_socket, user, password, secret, database, named_collection;
    std::filesystem::path user_files_dir, query_log_file;
    std::optional<Catalog> glue_catalog, hive_catalog, rest_catalog, unity_catalog;

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
    uint64_t threshold = 10, minimum = 1000;

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
    String schema_name, table_name;
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
    DB::Strings collations, storage_policies, timezones, disks, keeper_disks, clusters, caches, remote_servers, remote_secure_servers,
        http_servers, https_servers, arrow_flight_servers, hot_settings, disallowed_settings, hot_table_settings;
    std::optional<ServerCredentials> clickhouse_server, mysql_server, postgresql_server, sqlite_server, mongodb_server, redis_server,
        minio_server, http_server, azurite_server, dolor_server;
    std::unordered_map<String, PerformanceMetric> metrics;
    std::unordered_set<uint32_t> disallowed_error_codes, oracle_ignore_error_codes;
    String host = "localhost", keeper_map_path_prefix;
    bool read_log = false, fuzz_floating_points = true, test_with_fill = true, compare_success_results = false, measure_performance = false,
         allow_infinite_tables = false, compare_explains = false, allow_memory_tables = true, allow_client_restarts = false,
         enable_fault_injection_settings = false, enable_force_settings = false, allow_hardcoded_inserts = true, allow_async_requests = false,
         truncate_output = false, allow_transactions = true;
    uint64_t seed = 0, min_insert_rows = 1, max_insert_rows = 1000, min_nested_rows = 0, max_nested_rows = 10, flush_log_wait_time = 1000,
             type_mask = std::numeric_limits<uint64_t>::max(), engine_mask = std::numeric_limits<uint64_t>::max();
    uint32_t max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5, max_dictionaries = 5,
             max_columns = 5, time_to_run = 0, port = 9000, secure_port = 9440, http_port = 8123, http_secure_port = 8443,
             use_dump_table_oracle = 2, max_reconnection_attempts = 3, time_to_sleep_between_reconnects = 3000, min_string_length = 0,
             max_string_length = 1009, max_parallel_queries = 5, max_number_alters = 4;
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          client_file_path = "/var/lib/clickhouse/user_files", server_file_path = "/var/lib/clickhouse/user_files",
                          fuzz_client_out = client_file_path / "fuzz.data", fuzz_server_out = server_file_path / "fuzz.data",
                          lakes_path = "/var/lib/clickhouse/user_files/lakehouses";

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

    bool tableHasPartitions(bool detached, const String & database, const String & table);

    String tableGetRandomPartitionOrPart(uint64_t rand_val, bool detached, bool partition, const String & database, const String & table);

    void comparePerformanceResults(const String & oracle_name, PerformanceResult & server, PerformanceResult & peer) const;
};

}
