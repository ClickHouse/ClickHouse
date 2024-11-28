#pragma once

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

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

namespace BuzzHouse
{

class ServerCredentials
{
public:
    std::string hostname;
    uint32_t port;
    std::string unix_socket, user, password, database;
    std::filesystem::path query_log_file;

    ServerCredentials() : hostname("localhost"), port(0), user("test") { }

    ServerCredentials(
        const std::string & h,
        const uint32_t p,
        const std::string & us,
        const std::string & u,
        const std::string & pass,
        const std::string & db,
        const std::filesystem::path & qlf)
        : hostname(h), port(p), unix_socket(us), user(u), password(pass), database(db), query_log_file(qlf)
    {
    }

    ServerCredentials(const ServerCredentials & c) = default;
    ServerCredentials(ServerCredentials && c) = default;
    ServerCredentials & operator=(const ServerCredentials & c) = default;
    ServerCredentials & operator=(ServerCredentials && c) = default;
};

static std::optional<ServerCredentials>
loadServerCredentials(const JSONParserImpl::Element & jobj, const std::string & sname, const uint32_t & default_port)
{
    uint32_t port = default_port;
    std::string hostname = "localhost", unix_socket, user = "test", password, database = "test";
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    for (const auto [key, value] : jobj.getObject())
    {
        if (key == "hostname")
        {
            hostname = std::string(value.getString());
        }
        else if (key == "port")
        {
            port = static_cast<uint32_t>(value.getUInt64());
        }
        else if (key == "unix_socket")
        {
            unix_socket = std::string(value.getString());
        }
        else if (key == "user")
        {
            user = std::string(value.getString());
        }
        else if (key == "password")
        {
            password = std::string(value.getString());
        }
        else if (key == "database")
        {
            database = std::string(value.getString());
        }
        else if (key == "query_log_file")
        {
            query_log_file = std::filesystem::path(std::string(value.getString()));
        }
        else
        {
            throw std::runtime_error("Unknown option: " + std::string(key));
        }
    }
    return std::optional<ServerCredentials>(ServerCredentials(hostname, port, unix_socket, user, password, database, query_log_file));
}

class FuzzConfig
{
private:
    std::string buf;
    DB::ClientBase * cb = nullptr;

public:
    std::vector<std::string> collations;
    std::optional<ServerCredentials> clickhouse_server = std::nullopt, mysql_server = std::nullopt, postgresql_server = std::nullopt,
                                     sqlite_server = std::nullopt, mongodb_server = std::nullopt, redis_server = std::nullopt,
                                     minio_server = std::nullopt;
    bool read_log = false, fuzz_floating_points = true;
    uint64_t seed = 0;
    uint32_t max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5, time_to_run = 0;
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          db_file_path = std::filesystem::temp_directory_path() / "db", fuzz_out = db_file_path / "fuzz.data";

    FuzzConfig() : cb(nullptr) { buf.reserve(512); }

    FuzzConfig(DB::ClientBase * c, const std::string & path) : cb(c)
    {
        JSONParserImpl parser;
        JSONParserImpl::Element object;
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
        for (const auto [key, value] : object.getObject())
        {
            if (key == "db_file_path")
            {
                db_file_path = std::filesystem::path(std::string(value.getString()));
                fuzz_out = db_file_path / "fuzz.data";
            }
            else if (key == "log_path")
            {
                log_path = std::filesystem::path(std::string(value.getString()));
            }
            else if (key == "read_log")
            {
                read_log = value.getBool();
            }
            else if (key == "seed")
            {
                seed = value.getUInt64();
            }
            else if (key == "max_depth")
            {
                max_depth = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "max_width")
            {
                max_width = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "max_databases")
            {
                max_databases = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "max_functions")
            {
                max_functions = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "max_tables")
            {
                max_tables = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "max_views")
            {
                max_views = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "time_to_run")
            {
                time_to_run = static_cast<uint32_t>(value.getUInt64());
            }
            else if (key == "fuzz_floating_points")
            {
                fuzz_floating_points = value.getBool();
            }
            else if (key == "clickhouse")
            {
                clickhouse_server = loadServerCredentials(value, "clickhouse", 9004);
            }
            else if (key == "mysql")
            {
                mysql_server = loadServerCredentials(value, "mysql", 33060);
            }
            else if (key == "postgresql")
            {
                postgresql_server = loadServerCredentials(value, "postgresql", 5432);
            }
            else if (key == "sqlite")
            {
                sqlite_server = loadServerCredentials(value, "sqlite", 0);
            }
            else if (key == "mongodb")
            {
                mongodb_server = loadServerCredentials(value, "mongodb", 27017);
            }
            else if (key == "redis")
            {
                redis_server = loadServerCredentials(value, "redis", 6379);
            }
            else if (key == "minio")
            {
                minio_server = loadServerCredentials(value, "minio", 9000);
            }
            else
            {
                throw std::runtime_error("Unknown option: " + std::string(key));
            }
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

    void loadCollations()
    {
        buf.resize(0);
        buf += "SELECT \"name\" FROM system.collations INTO OUTFILE '";
        buf += fuzz_out.generic_string();
        buf += "' TRUNCATE FORMAT TabSeparated;";
        this->processServerQuery(buf);

        std::ifstream infile(fuzz_out);
        buf.resize(0);
        collations.clear();
        while (std::getline(infile, buf))
        {
            collations.push_back(buf);
            buf.resize(0);
        }
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
