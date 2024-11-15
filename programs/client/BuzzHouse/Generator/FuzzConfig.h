#pragma once

#include <filesystem>
#include <fstream>
#include <string>

#include "simdjson.h"

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

    ServerCredentials() : hostname("localhost"), port(0), unix_socket(""), user("test"), password(""), database(""), query_log_file("") { }

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

    ServerCredentials(const ServerCredentials & c)
    {
        this->hostname = c.hostname;
        this->port = c.port;
        this->user = c.user;
        this->unix_socket = c.unix_socket;
        this->password = c.password;
        this->database = c.database;
        this->query_log_file = c.query_log_file;
    }
    ServerCredentials(ServerCredentials && c)
    {
        this->hostname = c.hostname;
        this->port = c.port;
        this->user = c.user;
        this->unix_socket = c.unix_socket;
        this->password = c.password;
        this->database = c.database;
        this->query_log_file = c.query_log_file;
    }
    ServerCredentials & operator=(const ServerCredentials & c)
    {
        this->hostname = c.hostname;
        this->port = c.port;
        this->user = c.user;
        this->unix_socket = c.unix_socket;
        this->password = c.password;
        this->database = c.database;
        this->query_log_file = c.query_log_file;
        return *this;
    }
    ServerCredentials & operator=(ServerCredentials && c)
    {
        this->hostname = c.hostname;
        this->port = c.port;
        this->user = c.user;
        this->unix_socket = c.unix_socket;
        this->password = c.password;
        this->database = c.database;
        this->query_log_file = c.query_log_file;
        return *this;
    }
};

static const ServerCredentials
loadServerCredentials(const simdjson::dom::object & jobj, const std::string & sname, const uint32_t & default_port)
{
    uint32_t port = default_port;
    std::string hostname = "localhost", unix_socket = "", user = "test", password = "", database = "test";
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    for (const auto [key, value] : jobj)
    {
        if (key == "hostname")
        {
            hostname = std::string(value.get_c_str());
        }
        else if (key == "port")
        {
            port = static_cast<uint32_t>(value.get_int64());
        }
        else if (key == "unix_socket")
        {
            unix_socket = std::string(value.get_c_str());
        }
        else if (key == "user")
        {
            user = std::string(value.get_c_str());
        }
        else if (key == "password")
        {
            password = std::string(value.get_c_str());
        }
        else if (key == "database")
        {
            database = std::string(value.get_c_str());
        }
        else if (key == "query_log_file")
        {
            query_log_file = std::filesystem::path(std::string(value.get_c_str()));
        }
        else
        {
            throw std::runtime_error("Unknown option: " + std::string(key));
        }
    }
    return ServerCredentials(hostname, port, unix_socket, user, password, database, query_log_file);
}

class FuzzConfig
{
private:
    std::string buf;
    DB::ClientBase * cb = nullptr;

public:
    std::vector<const std::string> collations;
    ServerCredentials mysql_server, postgresql_server, sqlite_server, mongodb_server, redis_server, minio_server;
    bool read_log = false, fuzz_floating_points = true;
    uint32_t seed = 0, max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5;
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          db_file_path = std::filesystem::temp_directory_path() / "db", fuzz_out = db_file_path / "fuzz.data";

    FuzzConfig() : cb(nullptr), mysql_server(), postgresql_server(), sqlite_server(), mongodb_server(), redis_server(), minio_server()
    {
        buf.reserve(512);
    }

    FuzzConfig(DB::ClientBase * c, const std::string & path) : cb(c)
    {
        simdjson::dom::parser parser;
        simdjson::dom::object object;

        buf.reserve(512);
        auto error = parser.load(path).get(object);
        if (error)
        {
            throw std::runtime_error("Could not parse BuzzHouse configuration file: " + parser.get_error_message());
        }
        for (const auto [key, value] : object)
        {
            if (key == "db_file_path")
            {
                db_file_path = std::filesystem::path(std::string(value.get_c_str()));
                fuzz_out = db_file_path / "fuzz.data";
            }
            else if (key == "log_path")
            {
                log_path = std::filesystem::path(std::string(value.get_c_str()));
            }
            else if (key == "read_log")
            {
                read_log = value.get_bool();
            }
            else if (key == "seed")
            {
                seed = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_depth")
            {
                max_depth = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_width")
            {
                max_width = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_databases")
            {
                max_databases = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_functions")
            {
                max_functions = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_tables")
            {
                max_tables = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "max_views")
            {
                max_views = static_cast<uint32_t>(value.get_int64());
            }
            else if (key == "fuzz_floating_points")
            {
                fuzz_floating_points = value.get_bool();
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

    void processServerQuery(const std::string & input) const { this->cb->processTextAsSingleQuery(input); }

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
        buf += "SELECT count() FROM \"system\".\"";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (database != "")
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
        buf += "\" AS y FROM \"system\".\"";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (database != "")
        {
            buf += "\"database\" = '";
            buf += database;
            buf += "' AND ";
        }
        buf += "\"table\" = '";
        buf += table;
        buf += "' AND \"partition_id\" != 'all') AS z WHERE z.x = (SELECT rand() % (max2(count(), 1)::Int) FROM \"system\".\"";
        if constexpr (IsDetached)
        {
            buf += "detached_parts";
        }
        else
        {
            buf += "parts";
        }
        buf += "\" WHERE ";
        if (database != "")
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
