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

using IntegrationCall = enum IntegrationCall { MySQL = 1, PostgreSQL = 2, SQLite = 3, Redis = 4, MinIO = 5 };

class ClickHouseIntegration
{
public:
    std::string buf;
    std::ofstream out_file;

    ClickHouseIntegration() { buf.reserve(4096); }

    virtual bool PerformIntegration(RandomGenerator & rg, const uint32_t tname, std::vector<InsertEntry> & entries) = 0;

    virtual ~ClickHouseIntegration() = default;
};

class ClickHouseIntegratedDatabase : public ClickHouseIntegration
{
public:
    ClickHouseIntegratedDatabase() : ClickHouseIntegration() { }

    virtual bool PerformQuery(const std::string & buf) = 0;

    virtual std::string GetTableName(const uint32_t tname) = 0;

    virtual void GetTypeString(RandomGenerator & rg, const SQLType * tp, std::string & out) const = 0;

    bool PerformIntegration(RandomGenerator & rg, const uint32_t tname, std::vector<InsertEntry> & entries) override
    {
        bool res = true;
        const std::string str_tname = GetTableName(tname);

        buf.resize(0);
        buf += "DROP TABLE IF EXISTS ";
        buf += str_tname;
        buf += ";";

        if ((res = PerformQuery(buf)))
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
                GetTypeString(rg, entry.tp, buf);
                if (entry.nullable.has_value())
                {
                    buf += " ";
                    buf += entry.nullable.value() ? "" : "NOT ";
                    buf += "NULL";
                }
                assert(!entry.cname2.has_value());
            }
            buf += ");";
            res &= PerformQuery(buf);
        }
        return res;
    }

    ~ClickHouseIntegratedDatabase() override = default;
};

class MySQLIntegration : public ClickHouseIntegratedDatabase
{
private:
    MYSQL * mysql_connection = nullptr;

public:
    MySQLIntegration(const FuzzConfig & fc) : ClickHouseIntegratedDatabase()
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
            out_file = std::ofstream(fc.mysql_server.query_log_file, std::ios::out | std::ios::trunc);
            if (fc.read_log
                || (PerformQuery("DROP SCHEMA IF EXISTS " + fc.mysql_server.database + ";")
                    && PerformQuery("CREATE SCHEMA " + fc.mysql_server.database + ";")))
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

    std::string GetTableName(const uint32_t tname) override { return "test.t" + std::to_string(tname); }

    bool PerformQuery(const std::string & query) override
    {
        if (!mysql_connection)
        {
            std::cerr << "Not connected to MySQL" << std::endl;
            return false;
        }
        out_file << query << std::endl;
        if (mysql_query(mysql_connection, query.c_str()))
        {
            std::cerr << "MySQL query: " << query << std::endl << "Error: " << mysql_error(mysql_connection) << std::endl;
            return false;
        }
        return true;
    }

    void GetTypeString(RandomGenerator & rg, const SQLType * tp, std::string & out) const override { tp->MySQLTypeName(rg, out, false); }

    ~MySQLIntegration() override
    {
        if (mysql_connection)
        {
            mysql_close(mysql_connection);
        }
    }
};

class PostgreSQLIntegration : public ClickHouseIntegratedDatabase
{
private:
    pqxx::connection * postgres_connection = nullptr;

public:
    PostgreSQLIntegration(const FuzzConfig & fc) : ClickHouseIntegratedDatabase()
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
                out_file = std::ofstream(fc.postgresql_server.query_log_file, std::ios::out | std::ios::trunc);
                if (fc.read_log || (PerformQuery("DROP SCHEMA IF EXISTS test CASCADE;") && PerformQuery("CREATE SCHEMA test;")))
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

    std::string GetTableName(const uint32_t tname) override { return "test.t" + std::to_string(tname); }

    void GetTypeString(RandomGenerator & rg, const SQLType * tp, std::string & out) const override
    {
        tp->PostgreSQLTypeName(rg, out, false);
    }

    bool PerformQuery(const std::string & query) override
    {
        if (!postgres_connection)
        {
            std::cerr << "Not connected to PostgreSQL" << std::endl;
            return false;
        }
        try
        {
            pqxx::work w(*postgres_connection);

            out_file << query << std::endl;
            (void)w.exec(query.c_str());
            w.commit();
            return true;
        }
        catch (std::exception const & e)
        {
            std::cerr << "PostgreSQL query: " << query << std::endl << "Error: " << e.what() << std::endl;
            return false;
        }
    }

    ~PostgreSQLIntegration() override { delete postgres_connection; }
};

class SQLiteIntegration : public ClickHouseIntegratedDatabase
{
private:
    sqlite3 * sqlite_connection = nullptr;

public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(const FuzzConfig & fc) : ClickHouseIntegratedDatabase(), sqlite_path(fc.db_file_path / "sqlite.db")
    {
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
            out_file = std::ofstream(fc.sqlite_server.query_log_file, std::ios::out | std::ios::trunc);
            std::cout << "Connected to SQLite" << std::endl;
        }
    }

    std::string GetTableName(const uint32_t tname) override { return "t" + std::to_string(tname); }

    void GetTypeString(RandomGenerator & rg, const SQLType * tp, std::string & out) const override { tp->SQLiteTypeName(rg, out, false); }

    bool PerformQuery(const std::string & query) override
    {
        char * err_msg = NULL;

        if (!sqlite_connection)
        {
            std::cerr << "Not connected to SQLite" << std::endl;
            return false;
        }
        out_file << query << std::endl;
        if (sqlite3_exec(sqlite_connection, query.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
        {
            std::cerr << "SQLite query: " << query << std::endl << "Error: " << err_msg << std::endl;
            sqlite3_free(err_msg);
            return false;
        }
        return true;
    }

    ~SQLiteIntegration() override
    {
        if (sqlite_connection)
        {
            sqlite3_close(sqlite_connection);
        }
    }
};

class MinIOIntegration : public ClickHouseIntegration
{
private:
    const ServerCredentials & sc;
    bool SendRequest(const std::string & resource);

public:
    MinIOIntegration(const FuzzConfig & fc) : ClickHouseIntegration(), sc(fc.minio_server) { }

    bool PerformIntegration(RandomGenerator & rg, const uint32_t tname, std::vector<InsertEntry> & entries) override
    {
        (void)rg;
        (void)entries;
        return SendRequest(sc.database + "/file" + std::to_string(tname));
    }

    ~MinIOIntegration() override = default;
};

class RedisIntegration : public ClickHouseIntegration
{
public:
    RedisIntegration() : ClickHouseIntegration() { }

    bool PerformIntegration(RandomGenerator & rg, const uint32_t tname, std::vector<InsertEntry> & entries) override
    {
        (void)rg;
        (void)tname;
        (void)entries;
        return true;
    }

    ~RedisIntegration() override = default;
};

class ExternalIntegrations
{
private:
    MySQLIntegration * mysql = nullptr;
    PostgreSQLIntegration * postresql = nullptr;
    SQLiteIntegration * sqlite = nullptr;
    RedisIntegration * redis = nullptr;
    MinIOIntegration * minio = nullptr;
    bool requires_external_call_check = false, next_call_succeeded = false;

public:
    bool GetRequiresExternalCallCheck() const { return requires_external_call_check; }

    bool GetNextExternalCallSucceeded() const { return next_call_succeeded; }

    bool HasMySQLConnection() const { return mysql != nullptr; }

    bool HasPostgreSQLConnection() const { return postresql != nullptr; }

    bool HasSQLiteConnection() const { return sqlite != nullptr; }

    bool HasRedisConnection() const { return redis != nullptr; }

    bool HasMinIOConnection() const { return minio != nullptr; }

    const std::filesystem::path & GetSQLiteDBPath() { return sqlite->sqlite_path; }

    void ResetExternalStatus()
    {
        requires_external_call_check = false;
        next_call_succeeded = false;
    }

    ExternalIntegrations(const FuzzConfig & fc)
    {
        if (fc.mysql_server.port || fc.mysql_server.unix_socket != "")
        {
            mysql = new MySQLIntegration(fc);
        }
        if (fc.postgresql_server.port || fc.postgresql_server.unix_socket != "")
        {
            postresql = new PostgreSQLIntegration(fc);
        }
        sqlite = new SQLiteIntegration(fc);
        if (fc.redis_server.port)
        {
            redis = new RedisIntegration();
        }
        if (fc.minio_server.user != "" && fc.minio_server.password != "")
        {
            minio = new MinIOIntegration(fc);
        }
    }

    void
    CreateExternalDatabaseTable(RandomGenerator & rg, const IntegrationCall dc, const uint32_t tname, std::vector<InsertEntry> & entries)
    {
        requires_external_call_check = true;
        switch (dc)
        {
            case IntegrationCall::MySQL:
                next_call_succeeded = mysql->PerformIntegration(rg, tname, entries);
                break;
            case IntegrationCall::PostgreSQL:
                next_call_succeeded = postresql->PerformIntegration(rg, tname, entries);
                break;
            case IntegrationCall::SQLite:
                next_call_succeeded = sqlite->PerformIntegration(rg, tname, entries);
                break;
            case IntegrationCall::Redis:
                next_call_succeeded = redis->PerformIntegration(rg, tname, entries);
                break;
            case IntegrationCall::MinIO:
                next_call_succeeded = minio->PerformIntegration(rg, tname, entries);
                break;
        }
    }

    ~ExternalIntegrations()
    {
        delete mysql;
        delete postresql;
        delete sqlite;
        delete redis;
        delete minio;
    }
};

}
