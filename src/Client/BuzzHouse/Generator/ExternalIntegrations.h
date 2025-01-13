#pragma once

#include "config.h"

#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/SQLCatalog.h>

#if USE_MYSQL
#    if __has_include(<mysql.h>)
#        include <mysql.h>
#    else
#        include <mysql/mysql.h>
#    endif
#endif

#if USE_MONGODB
#    include <bsoncxx/builder/stream/array.hpp>
#    include <bsoncxx/builder/stream/document.hpp>
#    include <bsoncxx/json.hpp>
#    include <bsoncxx/types.hpp>
#    include <mongocxx/client.hpp>
#    include <mongocxx/collection.hpp>
#    include <mongocxx/database.hpp>
#endif

#if USE_LIBPQXX
#    include <pqxx/pqxx>
#endif

#if USE_SQLITE
#    include <sqlite3.h>
#endif


namespace BuzzHouse
{

enum class IntegrationCall
{
    MySQL = 1,
    PostgreSQL = 2,
    SQLite = 3,
    Redis = 4,
    MongoDB = 5,
    MinIO = 6
};

class ClickHouseIntegration
{
public:
    const FuzzConfig & fc;
    const ServerCredentials & sc;
    String buf;

    ClickHouseIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : fc(fcc), sc(scc) { buf.reserve(4096); }

    virtual void setEngineDetails(RandomGenerator &, const SQLBase &, const String &, TableEngine *) { }

    virtual bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t, bool, std::vector<ColumnPathChain> &)
    {
        return false;
    }

    virtual ~ClickHouseIntegration() = default;
};

class ClickHouseIntegratedDatabase : public ClickHouseIntegration
{
public:
    std::ofstream out_file;
    explicit ClickHouseIntegratedDatabase(const FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegration(fcc, scc), out_file(std::ofstream(scc.query_log_file, std::ios::out | std::ios::trunc))
    {
    }

    virtual bool performQuery(const String &) { return false; }

    virtual String getTableName(std::shared_ptr<SQLDatabase>, uint32_t) { return String(); }

    virtual void columnTypeAsString(RandomGenerator &, SQLType *, String &) const { }

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase> db,
        const uint32_t tname,
        const bool can_shuffle,
        std::vector<ColumnPathChain> & entries) override;

    bool dropPeerTableOnRemote(const SQLTable & t);

    virtual void optimizePeerTableOnRemote(const SQLTable &) { }

    bool performCreatePeerTable(
        RandomGenerator & rg,
        const bool is_clickhouse_integration,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries);

    void virtual truncateStatement(String &) { }

    void truncatePeerTableOnRemote(const SQLTable & t);

    ~ClickHouseIntegratedDatabase() override = default;
};

class MySQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_MYSQL && USE_MYSQL
private:
    const bool is_clickhouse;
    MYSQL * mysql_connection = nullptr;

public:
    MySQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, const bool is_click, MYSQL * mcon)
        : ClickHouseIntegratedDatabase(fcc, scc), is_clickhouse(is_click), mysql_connection(mcon)
    {
    }

    static MySQLIntegration *
    testAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log, const String & server);

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase> db, const uint32_t tname) override;

    void truncateStatement(String & outbuf) override;

    void optimizePeerTableOnRemote(const SQLTable & t) override;

    bool performQuery(const String & query) override;

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, String & out) const override;

    ~MySQLIntegration() override
    {
        if (mysql_connection)
        {
            mysql_close(mysql_connection);
        }
    }
#else
public:
    MySQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static MySQLIntegration * testAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials &, const bool, const String &);

    ~MySQLIntegration() override = default;
#endif
};

class PostgreSQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_LIBPQXX && USE_LIBPQXX
private:
    pqxx::connection * postgres_connection = nullptr;

public:
    PostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, pqxx::connection * pcon)
        : ClickHouseIntegratedDatabase(fcc, scc), postgres_connection(pcon)
    {
    }

    static PostgreSQLIntegration *
    testAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log);

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, const uint32_t tname) override;

    void truncateStatement(String & outbuf) override;

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, String & out) const override;

    bool performQuery(const String & query) override;

    ~PostgreSQLIntegration() override { delete postgres_connection; }
#else
public:
    PostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static PostgreSQLIntegration * testAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials &, const bool);

    ~PostgreSQLIntegration() override = default;
#endif
};

class SQLiteIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_SQLITE && USE_SQLITE
private:
    sqlite3 * sqlite_connection = nullptr;

public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, sqlite3 * scon, const std::filesystem::path & spath)
        : ClickHouseIntegratedDatabase(fcc, scc), sqlite_connection(scon), sqlite_path(spath)
    {
    }

    static SQLiteIntegration * testAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, const uint32_t tname) override;

    void truncateStatement(String & outbuf) override;

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, String & out) const override;

    bool performQuery(const String & query) override;

    ~SQLiteIntegration() override
    {
        if (sqlite_connection)
        {
            sqlite3_close(sqlite_connection);
        }
    }
#else
public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static SQLiteIntegration * testAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials &);

    ~SQLiteIntegration() override = default;
#endif
};

class RedisIntegration : public ClickHouseIntegration
{
public:
    RedisIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String &, TableEngine * te) override;

    bool performIntegration(
        RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t, const bool, std::vector<ColumnPathChain> &) override;

    ~RedisIntegration() override = default;
};

class MongoDBIntegration : public ClickHouseIntegration
{
#if defined USE_MONGODB && USE_MONGODB
private:
    std::ofstream out_file;
    std::vector<char> binary_data;
    std::vector<bsoncxx::document::value> documents;
    mongocxx::client client;
    mongocxx::database database;

    template <typename T>
    void documentAppendBottomType(RandomGenerator & rg, const String & cname, T & output, SQLType * tp);

    void documentAppendArray(RandomGenerator & rg, const String & cname, bsoncxx::builder::stream::document & document, ArrayType * at);
    void documentAppendAnyValue(RandomGenerator & rg, const String & cname, bsoncxx::builder::stream::document & document, SQLType * tp);

public:
    MongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, mongocxx::client & mcon, mongocxx::database & db)
        : ClickHouseIntegration(fcc, scc)
        , out_file(std::ofstream(scc.query_log_file, std::ios::out | std::ios::trunc))
        , client(std::move(mcon))
        , database(std::move(db))
    {
    }

    static MongoDBIntegration * testAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase>,
        const uint32_t tname,
        const bool can_shuffle,
        std::vector<ColumnPathChain> & entries) override;

    ~MongoDBIntegration() override = default;
#else
public:
    MongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    static MongoDBIntegration * testAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials &);

    ~MongoDBIntegration() override = default;
#endif
};

class MinIOIntegration : public ClickHouseIntegration
{
private:
    bool sendRequest(const String & resource);

public:
    explicit MinIOIntegration(const FuzzConfig & fcc, const ServerCredentials & ssc) : ClickHouseIntegration(fcc, ssc) { }

    void setEngineDetails(RandomGenerator &, const SQLBase & b, const String & tname, TableEngine * te) override;

    bool performIntegration(
        RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t tname, const bool, std::vector<ColumnPathChain> &) override;

    ~MinIOIntegration() override = default;
};

class ExternalIntegrations
{
private:
    MySQLIntegration * mysql = nullptr;
    PostgreSQLIntegration * postresql = nullptr;
    SQLiteIntegration * sqlite = nullptr;
    RedisIntegration * redis = nullptr;
    MongoDBIntegration * mongodb = nullptr;
    MinIOIntegration * minio = nullptr;
    MySQLIntegration * clickhouse = nullptr;

    std::filesystem::path default_sqlite_path;
    size_t requires_external_call_check = 0;
    std::vector<bool> next_calls_succeeded;

public:
    bool getRequiresExternalCallCheck() const { return requires_external_call_check > 0; }

    bool getNextExternalCallSucceeded() const
    {
        assert(requires_external_call_check == next_calls_succeeded.size());
        return std::all_of(next_calls_succeeded.begin(), next_calls_succeeded.end(), [](bool v) { return v; });
    }

    bool hasMySQLConnection() const { return mysql != nullptr; }

    bool hasPostgreSQLConnection() const { return postresql != nullptr; }

    bool hasSQLiteConnection() const { return sqlite != nullptr; }

    bool hasMongoDBConnection() const { return mongodb != nullptr; }

    bool hasRedisConnection() const { return redis != nullptr; }

    bool hasMinIOConnection() const { return minio != nullptr; }

    bool hasClickHouseExtraServerConnection() const { return clickhouse != nullptr; }

    const std::filesystem::path & getSQLitePath() const { return hasSQLiteConnection() ? sqlite->sqlite_path : default_sqlite_path; }

    void resetExternalStatus()
    {
        requires_external_call_check = 0;
        next_calls_succeeded.clear();
    }

    explicit ExternalIntegrations(const FuzzConfig & fc);

    void createExternalDatabaseTable(
        RandomGenerator & rg, const IntegrationCall dc, const SQLBase & b, std::vector<ColumnPathChain> & entries, TableEngine * te);

    void createPeerTable(
        RandomGenerator & rg,
        const PeerTableDatabase pt,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries);

    void truncatePeerTableOnRemote(const SQLTable & t);

    void optimizePeerTableOnRemote(const SQLTable & t);

    void dropPeerTableOnRemote(const SQLTable & t);

    bool performQuery(const PeerTableDatabase pt, const String & query);

    ~ExternalIntegrations()
    {
        delete mysql;
        delete postresql;
        delete sqlite;
        delete mongodb;
        delete redis;
        delete minio;
        delete clickhouse;
    }
};

}
