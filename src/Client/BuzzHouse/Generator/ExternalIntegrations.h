#pragma once

#include <base/defines.h>
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
#    include <mongocxx/exception/exception.hpp>
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
    MinIO = 6,
    Azurite = 7,
    HTTP = 8
};

class ClickHouseIntegration
{
public:
    FuzzConfig & fc;
    const ServerCredentials & sc;

    ClickHouseIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : fc(fcc)
        , sc(scc)
    {
    }

    virtual void setEngineDetails(RandomGenerator &, const SQLBase &, const String &, TableEngine *) { }

    virtual bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t, bool, bool, std::vector<ColumnPathChain> &)
    {
        return false;
    }

    virtual ~ClickHouseIntegration() = default;
};

class ClickHouseIntegratedDatabase : public ClickHouseIntegration
{
public:
    std::ofstream out_file;
    explicit ClickHouseIntegratedDatabase(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegration(fcc, scc)
        , out_file(std::ofstream(scc.query_log_file, std::ios::out | std::ios::trunc))
    {
    }

    virtual bool performQuery(const String &) { return false; }

    virtual String getTableName(std::shared_ptr<SQLDatabase>, uint32_t) { return String(); }

    virtual String columnTypeAsString(RandomGenerator &, bool, SQLType *) const { return String(); }

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase> db,
        uint32_t tname,
        bool can_shuffle,
        bool is_deterministic,
        std::vector<ColumnPathChain> & entries) override;

    bool dropPeerTableOnRemote(const SQLTable & t);

    virtual bool optimizeTableForOracle(PeerTableDatabase, const SQLTable &) { return true; }

    bool performCreatePeerTable(
        RandomGenerator & rg,
        bool is_clickhouse_integration,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries);

    virtual String truncateStatement() { return String(); }

    bool truncatePeerTableOnRemote(const SQLTable & t);

    bool performQueryOnServerOrRemote(PeerTableDatabase pt, const String & query);

    ~ClickHouseIntegratedDatabase() override = default;

private:
    void swapTableDefinitions(RandomGenerator & rg, CreateTable & newt);
};

class MySQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_MYSQL && USE_MYSQL
private:
    static void closeMySQLConnection(MYSQL * mysql);
    using MySQLUniqueKeyPtr = std::unique_ptr<MYSQL, decltype(&closeMySQLConnection)>;

    const bool is_clickhouse;
    MySQLUniqueKeyPtr mysql_connection;

public:
    MySQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc, const bool is_click, MySQLUniqueKeyPtr mcon)
        : ClickHouseIntegratedDatabase(fcc, scc)
        , is_clickhouse(is_click)
        , mysql_connection(std::move(mcon))
    {
    }

    static std::unique_ptr<MySQLIntegration>
    testAndAddMySQLConnection(FuzzConfig & fcc, const ServerCredentials & scc, bool read_log, const String & server);

    void setEngineDetails(RandomGenerator & rg, const SQLBase & b, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase> db, uint32_t tname) override;

    String truncateStatement() override;

    bool optimizeTableForOracle(PeerTableDatabase pt, const SQLTable & t) override;

    bool performQuery(const String & query) override;

    String columnTypeAsString(RandomGenerator & rg, bool is_deterministic, SQLType * tp) const override;
#else
public:
    MySQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegratedDatabase(fcc, scc)
    {
    }

    static std::unique_ptr<MySQLIntegration> testAndAddMySQLConnection(FuzzConfig & fcc, const ServerCredentials &, bool, const String &);
#endif
    ~MySQLIntegration() override = default;
};

class PostgreSQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_LIBPQXX && USE_LIBPQXX
private:
    static void closePostgreSQLConnection(pqxx::connection * psql);
    using PostgreSQLUniqueKeyPtr = std::unique_ptr<pqxx::connection, decltype(&closePostgreSQLConnection)>;

    PostgreSQLUniqueKeyPtr postgres_connection;

public:
    PostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc, PostgreSQLUniqueKeyPtr pcon)
        : ClickHouseIntegratedDatabase(fcc, scc)
        , postgres_connection(std::move(pcon))
    {
    }

    static std::unique_ptr<PostgreSQLIntegration>
    testAndAddPostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc, bool read_log);

    void setEngineDetails(RandomGenerator & rg, const SQLBase & b, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, uint32_t tname) override;

    String truncateStatement() override;

    String columnTypeAsString(RandomGenerator & rg, bool is_deterministic, SQLType * tp) const override;

    bool performQuery(const String & query) override;
#else
public:
    PostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegratedDatabase(fcc, scc)
    {
    }

    static std::unique_ptr<PostgreSQLIntegration> testAndAddPostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials &, bool);
#endif
    ~PostgreSQLIntegration() override = default;
};

class SQLiteIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_SQLITE && USE_SQLITE
private:
    static void closeSQLiteConnection(sqlite3 * sqlite);
    using SQLiteUniqueKeyPtr = std::unique_ptr<sqlite3, decltype(&closeSQLiteConnection)>;

    SQLiteUniqueKeyPtr sqlite_connection;

public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(FuzzConfig & fcc, const ServerCredentials & scc, SQLiteUniqueKeyPtr scon, const std::filesystem::path & spath)
        : ClickHouseIntegratedDatabase(fcc, scc)
        , sqlite_connection(std::move(scon))
        , sqlite_path(spath)
    {
    }

    static std::unique_ptr<SQLiteIntegration> testAndAddSQLiteIntegration(FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    String getTableName(std::shared_ptr<SQLDatabase>, uint32_t tname) override;

    String truncateStatement() override;

    String columnTypeAsString(RandomGenerator & rg, bool is_deterministic, SQLType * tp) const override;

    bool performQuery(const String & query) override;
#else
public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegratedDatabase(fcc, scc)
    {
    }

    static std::unique_ptr<SQLiteIntegration> testAndAddSQLiteIntegration(FuzzConfig & fcc, const ServerCredentials &);
#endif
    ~SQLiteIntegration() override = default;
};

class RedisIntegration : public ClickHouseIntegration
{
public:
    RedisIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegration(fcc, scc)
    {
    }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const String &, TableEngine * te) override;

    bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t, bool, bool, std::vector<ColumnPathChain> &) override;

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
    MongoDBIntegration(FuzzConfig & fcc, const ServerCredentials & scc, mongocxx::client & mcon, mongocxx::database & db)
        : ClickHouseIntegration(fcc, scc)
        , out_file(std::ofstream(scc.query_log_file, std::ios::out | std::ios::trunc))
        , client(std::move(mcon))
        , database(std::move(db))
    {
    }

    static std::unique_ptr<MongoDBIntegration> testAndAddMongoDBIntegration(FuzzConfig & fcc, const ServerCredentials & scc);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    bool performIntegration(
        RandomGenerator & rg,
        std::shared_ptr<SQLDatabase>,
        uint32_t tname,
        bool can_shuffle,
        bool is_deterministic,
        std::vector<ColumnPathChain> & entries) override;

    ~MongoDBIntegration() override = default;
#else
public:
    MongoDBIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
        : ClickHouseIntegration(fcc, scc)
    {
    }

    static std::unique_ptr<MongoDBIntegration> testAndAddMongoDBIntegration(FuzzConfig & fcc, const ServerCredentials &);

    ~MongoDBIntegration() override = default;
#endif
};

class MinIOIntegration : public ClickHouseIntegration
{
private:
    bool sendRequest(const String & resource);

public:
    explicit MinIOIntegration(FuzzConfig & fcc, const ServerCredentials & ssc)
        : ClickHouseIntegration(fcc, ssc)
    {
    }

    String getConnectionURL(bool client);

    void setEngineDetails(RandomGenerator &, const SQLBase & b, const String & tname, TableEngine * te) override;

    void setBackupDetails(const String & filename, BackupRestore * br);

    bool performIntegration(
        RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t tname, bool, bool, std::vector<ColumnPathChain> &) override;

    ~MinIOIntegration() override = default;
};

class AzuriteIntegration : public ClickHouseIntegration
{
public:
    explicit AzuriteIntegration(FuzzConfig & fcc, const ServerCredentials & ssc)
        : ClickHouseIntegration(fcc, ssc)
    {
    }

    void setEngineDetails(RandomGenerator &, const SQLBase & b, const String & tname, TableEngine * te) override;

    void setBackupDetails(const String & filename, BackupRestore * br);

    bool performIntegration(
        RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t tname, bool, bool, std::vector<ColumnPathChain> &) override;

    ~AzuriteIntegration() override = default;
};

class HTTPIntegration : public ClickHouseIntegration
{
public:
    explicit HTTPIntegration(FuzzConfig & fcc, const ServerCredentials & ssc)
        : ClickHouseIntegration(fcc, ssc)
    {
    }

    String getConnectionURL(bool client);

    void setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te) override;

    bool performIntegration(RandomGenerator &, std::shared_ptr<SQLDatabase>, uint32_t, bool, bool, std::vector<ColumnPathChain> &) override;

    ~HTTPIntegration() override = default;
};

class ExternalIntegrations
{
private:
    FuzzConfig & fc;
    std::unique_ptr<MySQLIntegration> mysql;
    std::unique_ptr<PostgreSQLIntegration> postresql;
    std::unique_ptr<SQLiteIntegration> sqlite;
    std::unique_ptr<RedisIntegration> redis;
    std::unique_ptr<MongoDBIntegration> mongodb;
    std::unique_ptr<MinIOIntegration> minio;
    std::unique_ptr<AzuriteIntegration> azurite;
    std::unique_ptr<HTTPIntegration> http;
    std::unique_ptr<MySQLIntegration> clickhouse;

    std::filesystem::path default_sqlite_path;
    size_t requires_external_call_check = 0;
    std::vector<bool> next_calls_succeeded;

    std::filesystem::path getDatabaseDataDir(PeerTableDatabase pt, bool server) const;

public:
    bool getRequiresExternalCallCheck() const { return requires_external_call_check > 0; }

    bool getNextExternalCallSucceeded() const
    {
        chassert(requires_external_call_check == next_calls_succeeded.size());
        return std::all_of(next_calls_succeeded.begin(), next_calls_succeeded.end(), [](bool v) { return v; });
    }

    bool hasMySQLConnection() const { return mysql != nullptr; }

    bool hasPostgreSQLConnection() const { return postresql != nullptr; }

    bool hasSQLiteConnection() const { return sqlite != nullptr; }

    bool hasMongoDBConnection() const { return mongodb != nullptr; }

    bool hasRedisConnection() const { return redis != nullptr; }

    bool hasMinIOConnection() const { return minio != nullptr; }

    bool hasAzuriteConnection() const { return azurite != nullptr; }

    bool hasHTTPConnection() const { return http != nullptr; }

    bool hasClickHouseExtraServerConnection() const { return clickhouse != nullptr; }

    const std::filesystem::path & getSQLitePath() const { return sqlite ? sqlite->sqlite_path : default_sqlite_path; }

    void resetExternalStatus()
    {
        requires_external_call_check = 0;
        next_calls_succeeded.clear();
    }

    explicit ExternalIntegrations(FuzzConfig & fcc);

    void createExternalDatabaseTable(
        RandomGenerator & rg, IntegrationCall dc, const SQLBase & b, std::vector<ColumnPathChain> & entries, TableEngine * te);

    void createPeerTable(
        RandomGenerator & rg, PeerTableDatabase pt, const SQLTable & t, const CreateTable * ct, std::vector<ColumnPathChain> & entries);

    bool truncatePeerTableOnRemote(const SQLTable & t);

    bool optimizeTableForOracle(PeerTableDatabase pt, const SQLTable & t);

    void dropPeerTableOnRemote(const SQLTable & t);

    bool performQuery(PeerTableDatabase pt, const String & query);

    bool getPerformanceMetricsForLastQuery(PeerTableDatabase pt, PerformanceResult & res);

    void setDefaultSettings(PeerTableDatabase pt, const DB::Strings & settings);

    void replicateSettings(PeerTableDatabase pt);

    void setBackupDetails(IntegrationCall dc, const String & filename, BackupRestore * br);
};

}
