#pragma once

#include "fuzz_config.h"
#include "sql_catalog.h"

#if __has_include(<mysql.h>)
#    include <mysql.h>
#else
#    include <mysql/mysql.h>
#endif
#include <pqxx/pqxx>
#include <sqlite3.h>


namespace buzzhouse
{

using DatabaseCall = enum DatabaseCall { MySQL = 1, PostgreSQL = 2, SQLite = 3 };

class ExternalDatabases
{
private:
    std::string buf;
    bool requires_external_call_check = false, next_call_succeeded = false;

    MYSQL * mysql_connection = nullptr;
    pqxx::connection * postgres_connection = nullptr;

public:
    const std::filesystem::path sqlite_path;

    bool HasMySQLConnection() const { return mysql_connection != nullptr; }

    bool HasPostgreSQLConnection() const { return postgres_connection != nullptr; }

    bool GetRequiresExternalCallCheck() const { return requires_external_call_check; }

    bool GetNextExternalCallSucceeded() const { return next_call_succeeded; }

    void ResetExternalStatus()
    {
        requires_external_call_check = false;
        next_call_succeeded = false;
    }

    ExternalDatabases(const FuzzConfig & fc) : sqlite_path(fc.db_file_path / "sqlite.db")
    {
        buf.reserve(4096);
        if (fc.mysql_server.port)
        {
            MYSQL * other = nullptr;

            if (!(other = mysql_init(nullptr)))
            {
                throw std::runtime_error("Out of memory");
            }
            if (!(mysql_connection = mysql_real_connect(
                      other,
                      fc.mysql_server.hostname.c_str(),
                      fc.mysql_server.user.c_str(),
                      fc.mysql_server.password.c_str(),
                      nullptr,
                      fc.mysql_server.port,
                      fc.mysql_server.unix_socket.c_str(),
                      0)))
            {
                mysql_close(other);
                throw std::runtime_error("Out of memory");
            }
            mysql_query(mysql_connection, "DROP SCHEMA IF EXISTS test;");
            mysql_query(mysql_connection, "CREATE SCHEMA test;");
        }
        if (fc.postgresql_server.port)
        {
            std::string connection_str = "";

            connection_str += "host=";
            connection_str += fc.postgresql_server.hostname;
            connection_str += " ";
            connection_str += "port=";
            connection_str += std::to_string(fc.postgresql_server.port);
            connection_str += " ";
            connection_str += "user=";
            connection_str += fc.postgresql_server.user;
            if (fc.postgresql_server.password != "")
            {
                connection_str += " ";
                connection_str += "password=";
                connection_str += fc.postgresql_server.password;
            }
            if (!(postgres_connection = new pqxx::connection(std::move(connection_str))))
            {
                throw std::runtime_error("Out of memory");
            }
            pqxx::work w(*postgres_connection);
            (void)w.exec("DROP SCHEMA IF EXISTS test;");
            (void)w.exec("CREATE SCHEMA test;");
            w.commit();
        }
    }

    ~ExternalDatabases()
    {
        if (mysql_connection)
        {
            mysql_close(mysql_connection);
        }
        delete postgres_connection;
    }

    void CreateExternalDatabaseTable(RandomGenerator & rg, const DatabaseCall dc, const uint32_t tname, std::vector<InsertEntry> & entries)
    {
        buf.resize(0);
        next_call_succeeded = false;
        requires_external_call_check = true;

        if (rg.NextSmallNumber() < 7)
        {
            std::shuffle(entries.begin(), entries.end(), rg.gen);
        }
        buf += "CREATE TABLE test.t";
        buf += std::to_string(tname);
        buf += "(";
        for (size_t i = 0; i < entries.size(); i++)
        {
            const InsertEntry & entry = entries[i];

            if (i != 0)
            {
                buf += ", ";
            }
            buf += "c";
            buf += std::to_string(entry.cname1);
            buf += " ";
            switch (dc)
            {
                case DatabaseCall::MySQL:
                    entry.tp->MySQLTypeName(rg, buf, false);
                    break;
                case DatabaseCall::PostgreSQL:
                    entry.tp->PostgreSQLTypeName(rg, buf, false);
                    break;
                default:
                    entry.tp->TypeName(buf, false);
            }
            assert(!entry.cname2.has_value());
        }
        buf += ");";

        switch (dc)
        {
            case DatabaseCall::MySQL:
                next_call_succeeded = mysql_connection && mysql_query(mysql_connection, buf.c_str()) == 0;
                break;
            case DatabaseCall::PostgreSQL:
                try
                {
                    pqxx::work w(*postgres_connection);
                    (void)w.exec(buf.c_str());
                    w.commit();
                    next_call_succeeded = true;
                }
                catch (...)
                {
                }
                break;
            default:
                break;
        }
    }
};

}
