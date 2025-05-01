#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>

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
                         set_no_decimal_limit = (1 << 27);

using JSONObjectType = JSONParserImpl::Element;

class ServerCredentials
{
public:
    String hostname;
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
        const String & h,
        const uint32_t p,
        const uint32_t mp,
        const String & us,
        const String & u,
        const String & pass,
        const String & db,
        const std::filesystem::path & ufd,
        const std::filesystem::path & qlf)
        : hostname(h)
        , port(p)
        , mysql_port(mp)
        , unix_socket(us)
        , user(u)
        , password(pass)
        , database(db)
        , user_files_dir(ufd)
        , query_log_file(qlf)
    {
    }

    ServerCredentials(const ServerCredentials & c) = default;
    ServerCredentials(ServerCredentials && c) = default;
    ServerCredentials & operator=(const ServerCredentials & c) = default;
    ServerCredentials & operator=(ServerCredentials && c) noexcept = default;
};

class FuzzConfig
{
private:
    DB::ClientBase * cb = nullptr;

public:
    LoggerPtr log;
    DB::Strings collations, storage_policies, timezones, disks, clusters;
    std::optional<ServerCredentials> clickhouse_server, mysql_server, postgresql_server, sqlite_server, mongodb_server, redis_server,
        minio_server;
    bool read_log = false, fuzz_floating_points = true, test_with_fill = true, use_dump_table_oracle = true,
         compare_success_results = false, measure_performance = false, allow_infinite_tables = false, compare_explains = false;
    uint64_t seed = 0, min_insert_rows = 1, max_insert_rows = 1000, min_nested_rows = 0, max_nested_rows = 10, query_time_threshold = 10,
             query_memory_threshold = 10, query_time_minimum = 2000, query_memory_minimum = 20000, flush_log_wait_time = 1000;
    uint32_t max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5, time_to_run = 0,
             type_mask = std::numeric_limits<uint32_t>::max();
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          db_file_path = std::filesystem::temp_directory_path() / "db", fuzz_out = db_file_path / "fuzz.data";

    FuzzConfig()
        : cb(nullptr)
        , log(getLogger("BuzzHouse"))
    {
    }

    FuzzConfig(DB::ClientBase * c, const String & path);

    bool processServerQuery(const String & input) const;

private:
    void loadServerSettings(DB::Strings & out, bool distinct, const String & table, const String & col) const;

public:
    void loadServerConfigurations();

    std::string getConnectionHostAndPort() const;

    void loadSystemTables(std::unordered_map<String, DB::Strings> & tables) const;

    bool tableHasPartitions(bool detached, const String & database, const String & table) const;

    String tableGetRandomPartitionOrPart(bool detached, bool partition, const String & database, const String & table) const;
};

}
