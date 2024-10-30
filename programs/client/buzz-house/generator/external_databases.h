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
    sqlite3 * sqlite_connection = nullptr;
    std::ofstream out_mysql, out_postgresql, out_sqlite;

public:
    const std::filesystem::path sqlite_path;

    bool HasMySQLConnection() const { return mysql_connection != nullptr; }

    bool HasPostgreSQLConnection() const { return postgres_connection != nullptr; }

    bool HasSQLiteConnection() const { return sqlite_connection != nullptr; }

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
        if (fc.mysql_server.port || fc.mysql_server.unix_socket != "")
        {
            if (!(mysql_connection = mysql_init(nullptr)))
            {
                std::cerr << "Could not initialize MySQL handle" << std::endl;
            }
            else if (!mysql_real_connect(
                         mysql_connection,
                         fc.mysql_server.hostname == "" ? nullptr : fc.mysql_server.hostname.c_str(),
                         fc.mysql_server.user == "" ? nullptr : fc.mysql_server.user.c_str(),
                         fc.mysql_server.password == "" ? nullptr : fc.mysql_server.password.c_str(),
                         nullptr,
                         fc.mysql_server.port,
                         fc.mysql_server.unix_socket == "" ? nullptr : fc.mysql_server.unix_socket.c_str(),
                         0))
            {
                std::cerr << "MySQL connection error: " << mysql_error(mysql_connection) << std::endl;
                mysql_close(mysql_connection);
                mysql_connection = nullptr;
            }
            else
            {
                out_mysql = std::ofstream(fc.mysql_server.query_log_file, std::ios::out | std::ios::trunc);
                if (fc.read_log ||
                    (PerformQuery(DatabaseCall::MySQL, "DROP SCHEMA IF EXISTS " + fc.mysql_server.database + ";") &&
                     PerformQuery(DatabaseCall::MySQL, "CREATE SCHEMA " + fc.mysql_server.database + ";")))
                {
                    std::cout << "Connected to MySQL" << std::endl;
                }
                else
                {
                    mysql_close(mysql_connection);
                    mysql_connection = nullptr;
                }
            }
        }
        if (fc.postgresql_server.port || fc.postgresql_server.unix_socket != "")
        {
            bool has_something = false;
            std::string connection_str = "";

            if (fc.postgresql_server.unix_socket != "" || fc.postgresql_server.hostname != "")
            {
                connection_str += "host='";
                connection_str += fc.postgresql_server.unix_socket != "" ? fc.postgresql_server.unix_socket : fc.postgresql_server.hostname;
                connection_str += "'";
                has_something = true;
            }
            if (fc.postgresql_server.port)
            {
                if (has_something)
                {
                    connection_str += " ";
                }
                connection_str += "port='";
                connection_str += std::to_string(fc.postgresql_server.port);
                connection_str += "'";
                has_something = true;
            }
            if (fc.postgresql_server.user != "")
            {
                if (has_something)
                {
                    connection_str += " ";
                }
                connection_str += "user='";
                connection_str += fc.postgresql_server.user;
                connection_str += "'";
                has_something = true;
            }
            if (fc.postgresql_server.password != "")
            {
                if (has_something)
                {
                    connection_str += " ";
                }
                connection_str += "password='";
                connection_str += fc.postgresql_server.password;
                connection_str += "'";
            }
            if (fc.postgresql_server.database != "")
            {
                if (has_something)
                {
                    connection_str += " ";
                }
                connection_str += "dbname='";
                connection_str += fc.postgresql_server.database;
                connection_str += "'";
            }
            try
            {
                if (!(postgres_connection = new pqxx::connection(std::move(connection_str))))
                {
                    std::cerr << "Could not initialize PostgreSQL handle" << std::endl;
                }
                else
                {
                    out_postgresql = std::ofstream(fc.postgresql_server.query_log_file, std::ios::out | std::ios::trunc);
                    if (fc.read_log ||
                        (PerformQuery(DatabaseCall::PostgreSQL, "DROP SCHEMA IF EXISTS test CASCADE;") &&
                         PerformQuery(DatabaseCall::PostgreSQL, "CREATE SCHEMA test;")))
                    {
                        std::cout << "Connected to PostgreSQL" << std::endl;
                    }
                    else
                    {
                        delete postgres_connection;
                        postgres_connection = nullptr;
                    }
                }
            }
            catch (std::exception const & e)
            {
                std::cerr << "PostgreSQL connection error: " << e.what() << std::endl;
                delete postgres_connection;
                postgres_connection = nullptr;
            }
        }
        if (sqlite3_open(sqlite_path.generic_string().c_str(), &sqlite_connection) != SQLITE_OK)
        {
            if (sqlite_connection)
            {
                std::cerr << "SQLite connection error: " << sqlite3_errmsg(sqlite_connection) << std::endl;
                sqlite3_close(sqlite_connection);
                sqlite_connection = nullptr;
            }
            else
            {
                std::cerr << "Could not initialize SQLite handle" << std::endl;
            }
        }
        else
        {
            out_sqlite = std::ofstream(fc.sqlite_server.query_log_file, std::ios::out | std::ios::trunc);
            std::cout << "Connected to SQLite" << std::endl;
        }
    }

    ~ExternalDatabases()
    {
        if (mysql_connection)
        {
            mysql_close(mysql_connection);
        }
        delete postgres_connection;
        if (sqlite_connection)
        {
            sqlite3_close(sqlite_connection);
        }
    }

    bool PerformQuery(const DatabaseCall dc, const std::string & query)
    {
        switch (dc)
        {
            case DatabaseCall::MySQL:
                if (!mysql_connection)
                {
                    std::cerr << "Not connected to MySQL" << std::endl;
                    return false;
                }
                out_mysql << query << std::endl;
                if (mysql_query(mysql_connection, query.c_str()))
                {
                    std::cerr << "MySQL query: " << query << std::endl << "Error: " << mysql_error(mysql_connection) << std::endl;
                    return false;
                }
                return true;
            case DatabaseCall::PostgreSQL:
                if (!postgres_connection)
                {
                    std::cerr << "Not connected to PostgreSQL" << std::endl;
                    return false;
                }
                try
                {
                    pqxx::work w(*postgres_connection);

                    out_postgresql << query << std::endl;
                    (void)w.exec(query.c_str());
                    w.commit();
                    return true;
                }
                catch (std::exception const & e)
                {
                    std::cerr << "PostgreSQL query: " << query << std::endl << "Error: " << e.what() << std::endl;
                    return false;
                }
            case DatabaseCall::SQLite: {
                char * err_msg = NULL;

                if (!sqlite_connection)
                {
                    std::cerr << "Not connected to SQLite" << std::endl;
                    return false;
                }
                out_sqlite << query << std::endl;
                if (sqlite3_exec(sqlite_connection, query.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
                {
                    std::cerr << "SQLite query: " << query << std::endl << "Error: " << err_msg << std::endl;
                    sqlite3_free(err_msg);
                    return false;
                }
                return true;
            }
        }
    }

    void CreateExternalDatabaseTable(RandomGenerator & rg, const DatabaseCall dc, const uint32_t tname, std::vector<InsertEntry> & entries)
    {
        const std::string str_tname = (dc == DatabaseCall::SQLite ? "t" : "test.t") + std::to_string(tname);

        requires_external_call_check = true;

        buf.resize(0);
        buf += "DROP TABLE IF EXISTS ";
        buf += str_tname;
        buf += ";";

        if ((next_call_succeeded = PerformQuery(dc, buf)))
        {
            buf.resize(0);
            buf += "CREATE TABLE ";
            buf += str_tname;
            buf += "(";

            if (rg.NextSmallNumber() < 7)
            {
                std::shuffle(entries.begin(), entries.end(), rg.gen);
            }
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
                    case DatabaseCall::SQLite:
                        entry.tp->SQLiteTypeName(rg, buf, false);
                        break;
                }
                if (entry.nullable.has_value())
                {
                    buf += " ";
                    buf += entry.nullable.value() ? "" : "NOT ";
                    buf += "NULL";
                }
                assert(!entry.cname2.has_value());
            }
            buf += ");";
            next_call_succeeded &= PerformQuery(dc, buf);
        }
    }
};

}
