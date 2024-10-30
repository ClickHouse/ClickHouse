#pragma once

#include <filesystem>
#include <fstream>
#include <string>

#include "../third_party/json.h"
using json = nlohmann::json;

namespace buzzhouse
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

static const ServerCredentials LoadServerCredentials(const json & val, const std::string & sname, const uint32_t & default_port)
{
    uint32_t port = default_port;
    std::string hostname = "localhost", unix_socket = "", user = "test", password = "", database = "test";
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    for (const auto & [key, value] : val.items())
    {
        if (key == "hostname")
        {
            hostname = static_cast<std::string>(value);
        }
        else if (key == "port")
        {
            port = static_cast<uint32_t>(value);
        }
        else if (key == "unix_socket")
        {
            unix_socket = static_cast<std::string>(value);
        }
        else if (key == "user")
        {
            user = static_cast<std::string>(value);
        }
        else if (key == "password")
        {
            password = static_cast<std::string>(value);
        }
        else if (key == "database")
        {
            database = static_cast<std::string>(value);
        }
        else if (key == "query_log_file")
        {
            query_log_file = std::filesystem::path(static_cast<std::string>(value));
        }
        else
        {
            throw std::runtime_error("Unknown option: " + key);
        }
    }
    return ServerCredentials(hostname, port, unix_socket, user, password, database, query_log_file);
}

class FuzzConfig
{
public:
    std::vector<const std::string> collations;
    ServerCredentials mysql_server, postgresql_server, sqlite_server;
    bool read_log = false, fuzz_floating_points = true;
    uint32_t seed = 0, max_depth = 3, max_width = 3, max_databases = 4, max_functions = 4, max_tables = 10, max_views = 5;
    std::filesystem::path log_path = std::filesystem::temp_directory_path() / "out.sql",
                          db_file_path = std::filesystem::temp_directory_path() / "db";

    FuzzConfig() : mysql_server(), postgresql_server() { }

    FuzzConfig(const std::string & path)
    {
        std::ifstream ifs(path);
        const json jdata = json::parse(ifs);

        for (const auto & [key, value] : jdata.items())
        {
            if (key == "db_file_path")
            {
                db_file_path = std::filesystem::path(value);
            }
            else if (key == "log_path")
            {
                log_path = std::filesystem::path(value);
            }
            else if (key == "read_log")
            {
                read_log = static_cast<bool>(value);
            }
            else if (key == "seed")
            {
                seed = static_cast<uint32_t>(value);
            }
            else if (key == "max_depth")
            {
                max_depth = static_cast<uint32_t>(value);
            }
            else if (key == "max_width")
            {
                max_width = static_cast<uint32_t>(value);
            }
            else if (key == "max_databases")
            {
                max_databases = static_cast<uint32_t>(value);
            }
            else if (key == "max_functions")
            {
                max_functions = static_cast<uint32_t>(value);
            }
            else if (key == "max_tables")
            {
                max_tables = static_cast<uint32_t>(value);
            }
            else if (key == "max_views")
            {
                max_views = static_cast<uint32_t>(value);
            }
            else if (key == "fuzz_floating_points")
            {
                fuzz_floating_points = static_cast<bool>(value);
            }
            else if (key == "mysql")
            {
                mysql_server = LoadServerCredentials(value, key, 33060);
            }
            else if (key == "postgresql")
            {
                postgresql_server = LoadServerCredentials(value, key, 5432);
            }
            else if (key == "sqlite")
            {
                sqlite_server = LoadServerCredentials(value, key, 0);
            }
            else
            {
                throw std::runtime_error("Unknown option: " + key);
            }
        }
    }

    void GenerateCollationsQuery(std::string & res) const
    {
        const std::filesystem::path & collfile = db_file_path / "collations.data";

        res += "SELECT \"name\" FROM system.collations INTO OUTFILE '";
        res += collfile.generic_string();
        res += "' TRUNCATE FORMAT TabSeparated;";
    }

    void LoadCollations()
    {
        std::string input;
        const std::filesystem::path & collfile = db_file_path / "collations.data";
        std::ifstream infile(collfile);

        input.reserve(64);
        collations.clear();
        while (std::getline(infile, input))
        {
            collations.push_back(input);
            input.resize(0);
        }
    }
};

}
