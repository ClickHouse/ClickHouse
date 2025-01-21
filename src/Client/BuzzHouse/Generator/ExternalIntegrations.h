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

    virtual String columnTypeAsString(RandomGenerator &, SQLType *) const { return String(); }

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase> db,
        uint32_t tname,
        bool can_shuffle,
        std::vector<ColumnPathChain> & entries) override;

    bool dropPeerTableOnRemote(const SQLTable & t);

    virtual void optimizeTableForOracle(PeerTableDatabase, const SQLTable &) { }

    bool performCreatePeerTable(
        RandomGenerator & rg,
        bool is_clickhouse_integration,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries);

    virtual String truncateStatement() { return String(); }

    void truncatePeerTableOnRemote(const SQLTable & t);

    void performQueryOnServerOrRemote(PeerTableDatabase pt, const String & query);

    ~ClickHouseIntegratedDatabase() override = default;
};

class MySQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_MYSQL && USE_MYSQL
private:
    const bool is_clickhouse;
    std::unique_ptr<MYSQL *> mysql_connection = nullptr;

public:
    MySQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, const bool is_click, std::unique_ptr<MYSQL *> mcon)
        : ClickHouseIntegratedDatabase(fcc, scc), is_clickhouse(is_click), mysql_connection(std::move(mcon))
    {
    }

    static std::unique_ptr<MySQLIntegration>
    testAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials & scc, bool read_log, const String & server);

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase> db, uint32_t tname) override;

    String truncateStatement() override;

    void optimizeTableForOracle(PeerTableDatabase pt, const SQLTable & t) override;

    bool performQuery(const String & query) override;

    String columnTypeAsString(RandomGenerator & rg, SQLType * tp) const override;

    ~MySQLIntegration() override
    {
        if (mysql_connection && *mysql_connection)
        {
            mysql_close(*mysql_connection);
        }
    }
#else
public:
    MySQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static std::unique_ptr<MySQLIntegration>
    testAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials &, bool, const String &);

    ~MySQLIntegration() override = default;
#endif
};

class PostgreSQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_LIBPQXX && USE_LIBPQXX
private:
    std::unique_ptr<pqxx::connection> postgres_connection = nullptr;

public:
    PostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, std::unique_ptr<pqxx::connection> pcon)
        : ClickHouseIntegratedDatabase(fcc, scc), postgres_connection(std::move(pcon))
    {
    }

    static std::unique_ptr<PostgreSQLIntegration>
    testAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, bool read_log);

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, uint32_t tname) override;

    String truncateStatement() override;

    String columnTypeAsString(RandomGenerator & rg, SQLType * tp) const override;

    bool performQuery(const String & query) override;
#else
public:
    PostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static std::unique_ptr<PostgreSQLIntegration> testAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials &, bool);

    ~PostgreSQLIntegration() override = default;
#endif
};

class SQLiteIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_SQLITE && USE_SQLITE
private:
    std::unique_ptr<sqlite3 *> sqlite_connection = nullptr;

public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(
        const FuzzConfig & fcc, const ServerCredentials & scc, std::unique_ptr<sqlite3 *> scon, const std::filesystem::path & spath)
        : ClickHouseIntegratedDatabase(fcc, scc), sqlite_connection(std::move(scon)), sqlite_path(spath)
    {
    }

    static std::unique_ptr<SQLiteIntegration> testAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, uint32_t tname) override;

    String truncateStatement() override;

    String columnTypeAsString(RandomGenerator & rg, SQLType * tp) const override;

    bool performQuery(const String & query) override;

    ~SQLiteIntegration() override
    {
        if (sqlite_connection && *sqlite_connection)
        {
            sqlite3_close(*sqlite_connection);
        }
    }
#else
public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static std::unique_ptr<SQLiteIntegration> testAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials &);

    ~SQLiteIntegration() override = default;
#endif
};

class RedisIntegration : public ClickHouseIntegration
{
public:
    RedisIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String &, TableEngine * te) override;

    bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t, bool, std::vector<ColumnPathChain> &) override;

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

    static std::unique_ptr<MongoDBIntegration> testAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase>,
        uint32_t tname,
        bool can_shuffle,
        std::vector<ColumnPathChain> & entries) override;

    ~MongoDBIntegration() override = default;
#else
public:
    MongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    static std::unique_ptr<MongoDBIntegration> testAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials &);

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

    bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t tname, bool, std::vector<ColumnPathChain> &) override;

    ~MinIOIntegration() override = default;
};

class ExternalIntegrations
{
private:
    const FuzzConfig & fc;
    String buf;
    std::unique_ptr<MySQLIntegration> mysql = nullptr;
    std::unique_ptr<PostgreSQLIntegration> postresql = nullptr;
    std::unique_ptr<SQLiteIntegration> sqlite = nullptr;
    std::unique_ptr<RedisIntegration> redis = nullptr;
    std::unique_ptr<MongoDBIntegration> mongodb = nullptr;
    std::unique_ptr<MinIOIntegration> minio = nullptr;
    std::unique_ptr<MySQLIntegration> clickhouse = nullptr;

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

    explicit ExternalIntegrations(const FuzzConfig & fcc);

    void createExternalDatabaseTable(
        RandomGenerator & rg, IntegrationCall dc, const SQLBase & b, std::vector<ColumnPathChain> & entries, TableEngine * te);

    void createPeerTable(
        RandomGenerator & rg, PeerTableDatabase pt, const SQLTable & t, const CreateTable * ct, std::vector<ColumnPathChain> & entries);

    void truncatePeerTableOnRemote(const SQLTable & t);

    void optimizeTableForOracle(PeerTableDatabase pt, const SQLTable & t);

    void dropPeerTableOnRemote(const SQLTable & t);

    bool performQuery(PeerTableDatabase pt, const String & query);

    void getPerformanceMetricsForLastQuery(PeerTableDatabase pt, uint64_t & query_duration_ms, uint64_t & memory_usage);

    void setDefaultSettings(PeerTableDatabase pt, const std::vector<String> & settings);

    void replicateSettings(PeerTableDatabase pt);
};

}
