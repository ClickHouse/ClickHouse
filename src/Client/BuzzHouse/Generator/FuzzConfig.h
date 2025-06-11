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

#include <Client/ClientBase.h>
#include <Common/logger_useful.h>

namespace BuzzHouse
{

const constexpr uint32_t allow_bool = (1 << 0), allow_unsigned_int = (1 << 1), allow_int8 = (1 << 2), allow_int64 = (1 << 3),
                         allow_int128 = (1 << 4), allow_floating_points = (1 << 5), allow_dates = (1 << 6), allow_date32 = (1 << 7),
                         allow_datetimes = (1 << 8), allow_datetime64 = (1 << 9), allow_strings = (1 << 10), allow_decimals = (1 << 11),
                         allow_uuid = (1 << 12), allow_enum = (1 << 13), allow_dynamic = (1 << 14), allow_JSON = (1 << 15),
                         allow_nullable = (1 << 16), allow_low_cardinality = (1 << 17), allow_array = (1 << 18), allow_map = (1 << 19),
                         allow_tuple = (1 << 20), allow_variant = (1 << 21), allow_nested = (1 << 22), allow_ipv4 = (1 << 23),
                         allow_ipv6 = (1 << 24), allow_geo = (1 << 25), set_any_datetime_precision = (1 << 26),
                         set_no_decimal_limit = (1 << 27), allow_fixed_strings = (1 << 28), allow_time = (1 << 29),
                         allow_time64 = (1 << 30);

using JSONObjectType = JSONParserImpl::Element;

class ServerCredentials
{
public:
    String hostname, container;
    uint32_t port, mysql_port;
    String unix_socket, user, password, database;
    std::filesystem::path user_files_dir, query_log_file;

    ServerCredentials()
        : hostname("localhost")
        , port(0)
        , mysql_port(0)
        , user("test")
    {
    }

    ServerCredentials(
        const String & hostname_,
        const String & container_,
        const uint32_t port_,
        const uint32_t mysql_port_,
        const String & unix_socket_,
        const String & user_,
        const String & password_,
        const String & database_,
        const std::filesystem::path & user_files_dir_,
        const std::filesystem::path & query_log_file_)
        : hostname(hostname_)
        , container(container_)
        , port(port_)
        , mysql_port(mysql_port_)
        , unix_socket(unix_socket_)
        , user(user_)
        , password(password_)
        , database(database_)
        , user_files_dir(user_files_dir_)
        , query_log_file(query_log_file_)
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

class FuzzConfig
{
private:
    DB::ClientBase * cb = nullptr;

public:
    LoggerPtr log;
    std::ofstream outf;
    DB::Strings collations, storage_policies, timezones, disks, keeper_disks, clusters;
    std::optional<ServerCredentials> clickhouse_server, mysql_server, postgresql_server, sqlite_server, mongodb_server, redis_server,
        minio_server, http_server, azurite_server;
    std::unordered_map<String, PerformanceMetric> metrics;
    std::unordered_set<uint32_t> disallowed_error_codes;
    String host = "localhost", keeper_map_path_prefix;
    bool read_log = false, fuzz_floating_points = true, test_with_fill = true, dump_table_oracle_compare_content = true,
         compare_success_results = false, measure_performance = false, allow_infinite_tables = false, compare_explains = false,
         allow_memory_tables = true, allow_client_restarts = false;
    uint64_t seed = 0, min_insert_rows = 1, max_insert_rows = 1000, min_nested_rows = 0, max_nested_rows = 10, flush_log_wait_time = 1000;
    uint32_t max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5, max_dictionaries = 5,
             max_columns = 5, time_to_run = 0, type_mask = std::numeric_limits<uint32_t>::max(), port = 9000, secure_port = 9440,
             http_port = 8123, use_dump_table_oracle = 2, max_reconnection_attempts = 3, time_to_sleep_between_reconnects = 3000;
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          client_file_path = std::filesystem::temp_directory_path() / "db",
                          server_file_path = std::filesystem::temp_directory_path() / "db",
                          fuzz_client_out = client_file_path / "fuzz.data", fuzz_server_out = server_file_path / "fuzz.data";

    FuzzConfig()
        : cb(nullptr)
        , log(getLogger("BuzzHouse"))
    {
    }

    FuzzConfig(DB::ClientBase * c, const String & path);

    bool processServerQuery(bool outlog, const String & query);

private:
    void loadServerSettings(DB::Strings & out, bool distinct, const String & table, const String & col, const String & extra_clause);

public:
    void loadServerConfigurations();

    String getConnectionHostAndPort(bool secure) const;

    void loadSystemTables(std::unordered_map<String, DB::Strings> & tables);

    bool tableHasPartitions(bool detached, const String & database, const String & table);

    String tableGetRandomPartitionOrPart(bool detached, bool partition, const String & database, const String & table);

    void comparePerformanceResults(const String & oracle_name, PerformanceResult & server, PerformanceResult & peer) const;
};

}
