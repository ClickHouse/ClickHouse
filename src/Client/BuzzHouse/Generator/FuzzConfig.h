#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

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
                         allow_ipv6 = (1 << 24), allow_geo = (1 << 25), set_any_datetime_precision = (1 << 26);

static inline std::vector<std::string> splitString(const std::string & input, const char delimiter)
{
    std::vector<std::string> result;
    const char * str = input.c_str();

    do
    {
        const char * begin = str;

        while (*str && *str != delimiter)
        {
            str++;
        }
        result.push_back(std::string(begin, str));
    } while (*str++);

    return result;
}

using JSONObjectType = JSONParserImpl::Element;

class ServerCredentials
{
public:
    std::string hostname;
    uint32_t port, mysql_port;
    std::string unix_socket, user, password, database;
    std::filesystem::path query_log_file;

    ServerCredentials() : hostname("localhost"), port(0), mysql_port(0), user("test") { }

    ServerCredentials(
        const std::string & h,
        const uint32_t p,
        const uint32_t mp,
        const std::string & us,
        const std::string & u,
        const std::string & pass,
        const std::string & db,
        const std::filesystem::path & qlf)
        : hostname(h), port(p), mysql_port(mp), unix_socket(us), user(u), password(pass), database(db), query_log_file(qlf)
    {
    }

    ServerCredentials(const ServerCredentials & c) = default;
    ServerCredentials(ServerCredentials && c) = default;
    ServerCredentials & operator=(const ServerCredentials & c) = default;
    ServerCredentials & operator=(ServerCredentials && c) noexcept = default;
};

static std::optional<ServerCredentials> loadServerCredentials(
    const JSONParserImpl::Element & jobj, const std::string & sname, const uint32_t default_port, const uint32_t default_mysql_port = 0)
{
    uint32_t port = default_port;
    uint32_t mysql_port = default_mysql_port;
    std::string hostname = "localhost";
    std::string unix_socket;
    std::string user = "test";
    std::string password;
    std::string database = "test";
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    std::map<std::string, std::function<void(const JSONObjectType &)>> config_entries = {
        {"hostname", [&](const JSONObjectType & value) { hostname = std::string(value.getString()); }},
        {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }},
        {"mysql_port", [&](const JSONObjectType & value) { mysql_port = static_cast<uint32_t>(value.getUInt64()); }},
        {"unix_socket", [&](const JSONObjectType & value) { unix_socket = std::string(value.getString()); }},
        {"user", [&](const JSONObjectType & value) { user = std::string(value.getString()); }},
        {"password", [&](const JSONObjectType & value) { password = std::string(value.getString()); }},
        {"database", [&](const JSONObjectType & value) { database = std::string(value.getString()); }},
        {"query_log_file", [&](const JSONObjectType & value) { query_log_file = std::filesystem::path(std::string(value.getString())); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const std::string & nkey = std::string(key);

        if (config_entries.find(nkey) == config_entries.end())
        {
            throw std::runtime_error("Unknown Server option: " + nkey);
        }
        config_entries.at(nkey)(value);
    }

    return std::optional<ServerCredentials>(
        ServerCredentials(hostname, port, mysql_port, unix_socket, user, password, database, query_log_file));
}

class FuzzConfig
{
private:
    std::string buf;
    DB::ClientBase * cb = nullptr;

public:
    LoggerPtr log;
    std::vector<std::string> collations, storage_policies, timezones, disks;
    std::optional<ServerCredentials> clickhouse_server = std::nullopt, mysql_server = std::nullopt, postgresql_server = std::nullopt,
                                     sqlite_server = std::nullopt, mongodb_server = std::nullopt, redis_server = std::nullopt,
                                     minio_server = std::nullopt;
    bool read_log = false, fuzz_floating_points = true, test_with_fill = true, use_dump_table_oracle = true;
    uint64_t seed = 0, min_insert_rows = 1, max_insert_rows = 1000, min_nested_rows = 1, max_nested_rows = 10;
    uint32_t max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5, time_to_run = 0,
             type_mask = std::numeric_limits<uint32_t>::max();
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          db_file_path = std::filesystem::temp_directory_path() / "db", fuzz_out = db_file_path / "fuzz.data";

    FuzzConfig() : cb(nullptr), log(getLogger("BuzzHouse")) { buf.reserve(512); }

    FuzzConfig(DB::ClientBase * c, const std::string & path) : cb(c), log(getLogger("BuzzHouse"))
    {
        JSONParserImpl parser;
        JSONObjectType object;
        std::ifstream inputFile(path);
        std::string fileContent;

        buf.reserve(512);
        while (std::getline(inputFile, buf))
        {
            fileContent += buf;
        }
        inputFile.close();
        if (!parser.parse(fileContent, object))
        {
            throw std::runtime_error("Could not parse BuzzHouse JSON configuration file");
        }
        else if (!object.isObject())
        {
            throw std::runtime_error("Parsed JSON value is not an object");
        }

        std::map<std::string, std::function<void(const JSONObjectType &)>> config_entries
            = {{"db_file_path",
                [&](const JSONObjectType & value)
                {
                    db_file_path = std::filesystem::path(std::string(value.getString()));
                    fuzz_out = db_file_path / "fuzz.data";
                }},
               {"log_path", [&](const JSONObjectType & value) { log_path = std::filesystem::path(std::string(value.getString())); }},
               {"read_log", [&](const JSONObjectType & value) { read_log = value.getBool(); }},
               {"seed", [&](const JSONObjectType & value) { seed = value.getUInt64(); }},
               {"min_insert_rows", [&](const JSONObjectType & value) { min_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
               {"max_insert_rows", [&](const JSONObjectType & value) { max_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
               {"min_nested_rows", [&](const JSONObjectType & value) { min_nested_rows = value.getUInt64(); }},
               {"max_nested_rows", [&](const JSONObjectType & value) { max_nested_rows = value.getUInt64(); }},
               {"max_depth",
                [&](const JSONObjectType & value) { max_depth = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
               {"max_width",
                [&](const JSONObjectType & value) { max_width = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
               {"max_databases", [&](const JSONObjectType & value) { max_databases = static_cast<uint32_t>(value.getUInt64()); }},
               {"max_functions", [&](const JSONObjectType & value) { max_functions = static_cast<uint32_t>(value.getUInt64()); }},
               {"max_tables", [&](const JSONObjectType & value) { max_tables = static_cast<uint32_t>(value.getUInt64()); }},
               {"max_views", [&](const JSONObjectType & value) { max_views = static_cast<uint32_t>(value.getUInt64()); }},
               {"time_to_run", [&](const JSONObjectType & value) { time_to_run = static_cast<uint32_t>(value.getUInt64()); }},
               {"fuzz_floating_points", [&](const JSONObjectType & value) { fuzz_floating_points = value.getBool(); }},
               {"test_with_fill", [&](const JSONObjectType & value) { test_with_fill = value.getBool(); }},
               {"use_dump_table_oracle", [&](const JSONObjectType & value) { use_dump_table_oracle = value.getBool(); }},
               {"clickhouse",
                [&](const JSONObjectType & value) { clickhouse_server = loadServerCredentials(value, "clickhouse", 9004, 9005); }},
               {"mysql", [&](const JSONObjectType & value) { mysql_server = loadServerCredentials(value, "mysql", 3306, 3306); }},
               {"postgresql", [&](const JSONObjectType & value) { postgresql_server = loadServerCredentials(value, "postgresql", 5432); }},
               {"sqlite", [&](const JSONObjectType & value) { sqlite_server = loadServerCredentials(value, "sqlite", 0); }},
               {"mongodb", [&](const JSONObjectType & value) { mongodb_server = loadServerCredentials(value, "mongodb", 27017); }},
               {"redis", [&](const JSONObjectType & value) { redis_server = loadServerCredentials(value, "redis", 6379); }},
               {"minio", [&](const JSONObjectType & value) { minio_server = loadServerCredentials(value, "minio", 9000); }},
               {"disabled_types",
                [&](const JSONObjectType & value)
                {
                    std::string input = std::string(value.getString());
                    std::transform(input.begin(), input.end(), input.begin(), ::tolower);
                    const auto & split = splitString(input, ',');

                    std::map<std::string, uint32_t> type_entries
                        = {{"bool", allow_bool},
                           {"uint", allow_unsigned_int},
                           {"int8", allow_int8},
                           {"int64", allow_int64},
                           {"int128", allow_int128},
                           {"float", allow_floating_points},
                           {"date", allow_dates},
                           {"date32", allow_date32},
                           {"datetime", allow_datetimes},
                           {"datetime64", allow_datetime64},
                           {"string", allow_strings},
                           {"decimal", allow_decimals},
                           {"uuid", allow_uuid},
                           {"enum", allow_enum},
                           {"uuid", allow_uuid},
                           {"dynamic", allow_dynamic},
                           {"json", allow_JSON},
                           {"nullable", allow_nullable},
                           {"lcard", allow_low_cardinality},
                           {"array", allow_array},
                           {"map", allow_map},
                           {"tuple", allow_tuple},
                           {"variant", allow_variant},
                           {"nested", allow_nested},
                           {"ipv4", allow_ipv4},
                           {"ipv6", allow_ipv6},
                           {"geo", allow_geo}};

                    for (const auto & entry : split)
                    {
                        if (type_entries.find(entry) == type_entries.end())
                        {
                            throw std::runtime_error("Unknown type optiom: " + entry);
                        }
                        type_mask &= (~type_entries.at(entry));
                    }
                }}};

        for (const auto [key, value] : object.getObject())
        {
            const std::string & nkey = std::string(key);

            if (config_entries.find(nkey) == config_entries.end())
            {
                throw std::runtime_error("Unknown BuzzHouse option: " + nkey);
            }
            config_entries.at(nkey)(value);
        }
        if (min_insert_rows > max_insert_rows)
        {
            throw std::runtime_error(
                "min_insert_rows value (" + std::to_string(min_insert_rows) + ") is higher than max_insert_rows value ("
                + std::to_string(max_insert_rows) + ")");
        }
        if (min_nested_rows > max_nested_rows)
        {
            throw std::runtime_error(
                "min_nested_rows value (" + std::to_string(min_nested_rows) + ") is higher than max_nested_rows value ("
                + std::to_string(max_nested_rows) + ")");
        }
    }

    bool processServerQuery(const std::string & input) const
    {
        try
        {
            this->cb->processTextAsSingleQuery(input);
        }
        catch (...)
        {
            return false;
        }
        return true;
    }

private:
    void loadServerSettings(std::vector<std::string> & out, const std::string & table, const std::string & col)
    {
        uint64_t found = 0;

        buf.resize(0);
        buf += "SELECT \"";
        buf += col;
        buf += R"(" FROM "system".")";
        buf += table;
        buf += "\" INTO OUTFILE '";
        buf += fuzz_out.generic_string();
        buf += "' TRUNCATE FORMAT TabSeparated;";
        this->processServerQuery(buf);

        std::ifstream infile(fuzz_out);
        buf.resize(0);
        out.clear();
        while (std::getline(infile, buf))
        {
            out.push_back(buf);
            buf.resize(0);
            found++;
        }
        LOG_INFO(log, "Found {} entries from {} table", found, table);
    }

public:
    void loadServerConfigurations()
    {
        loadServerSettings(this->collations, "collations", "name");
        loadServerSettings(this->storage_policies, "storage_policies", "policy_name");
        loadServerSettings(this->disks, "disks", "name");
        loadServerSettings(this->timezones, "time_zones", "time_zone");
    }

    template <bool IsDetached>
    bool tableHasPartitions(const std::string & database, const std::string & table)
    {
        buf.resize(0);
        buf += R"(SELECT count() FROM "system".")";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (!database.empty())
        {
            buf += "\"database\" = '";
            buf += database;
            buf += "' AND ";
        }
        buf += "\"table\" = '";
        buf += table;
        buf += "' AND \"partition_id\" != 'all' INTO OUTFILE '";
        buf += fuzz_out.generic_string();
        buf += "' TRUNCATE FORMAT CSV;";
        this->processServerQuery(buf);

        std::ifstream infile(fuzz_out);
        buf.resize(0);
        if (std::getline(infile, buf))
        {
            return !buf.empty() && buf[0] != '0';
        }
        return false;
    }

    template <bool IsDetached, bool IsPartition>
    void tableGetRandomPartitionOrPart(const std::string & database, const std::string & table, std::string & res)
    {
        //system.parts doesn't support sampling, so pick up a random part with a window function
        buf.resize(0);
        buf += "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"";
        if constexpr (IsPartition)
        {
            buf += "partition_id";
        }
        else
        {
            buf += "name";
        }
        buf += R"(" AS y FROM "system".")";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (!database.empty())
        {
            buf += "\"database\" = '";
            buf += database;
            buf += "' AND ";
        }
        buf += "\"table\" = '";
        buf += table;
        buf += R"(' AND "partition_id" != 'all') AS z WHERE z.x = (SELECT rand() % (max2(count(), 1)::Int) FROM "system".")";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (!database.empty())
        {
            buf += "\"database\" = '";
            buf += database;
            buf += "' AND ";
        }
        buf += "\"table\" = '";
        buf += table;
        buf += "') INTO OUTFILE '";
        buf += fuzz_out.generic_string();
        buf += "' TRUNCATE FORMAT RawBlob;";
        this->processServerQuery(buf);

        res.resize(0);
        std::ifstream infile(fuzz_out, std::ios::in);
        std::getline(infile, res);
    }
};

}
