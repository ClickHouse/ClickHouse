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
    std::string buf;

    ClickHouseIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : fc(fcc), sc(scc) { buf.reserve(4096); }

    virtual void setEngineDetails(RandomGenerator &, const SQLBase &, const std::string &, TableEngine *) { }

    virtual bool performIntegration(RandomGenerator &, uint32_t, bool, std::vector<ColumnPathChain> &) { return false; }

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

    virtual bool performQuery(const std::string &) { return false; }

    virtual std::string getTableName(uint32_t) { return std::string(); }

    virtual void columnTypeAsString(RandomGenerator &, SQLType *, std::string &) const { }

    bool
    performIntegration(RandomGenerator & rg, const uint32_t tname, const bool can_shuffle, std::vector<ColumnPathChain> & entries) override
    {
        const std::string str_tname = getTableName(tname);

        buf.resize(0);
        buf += "DROP TABLE IF EXISTS ";
        buf += str_tname;
        buf += ";";

        if (performQuery(buf))
        {
            bool first = true;

            buf.resize(0);
            buf += "CREATE TABLE ";
            buf += str_tname;
            buf += "(";

            if (can_shuffle && rg.nextSmallNumber() < 7)
            {
                std::shuffle(entries.begin(), entries.end(), rg.generator);
            }
            for (const auto & entry : entries)
            {
                SQLType * tp = entry.getBottomType();

                if (!first)
                {
                    buf += ", ";
                }
                buf += entry.getBottomName();
                buf += " ";
                columnTypeAsString(rg, tp, buf);
                buf += " ";
                buf += ((entry.nullable.has_value() && entry.nullable.value()) || hasType<Nullable, false, false, false>(tp)) ? "" : "NOT ";
                buf += "NULL";
                assert(entry.path.size() == 1);
                first = false;
            }
            buf += ");";
            return performQuery(buf);
        }
        return false;
    }

    bool dropPeerTableOnRemote(const SQLTable & t)
    {
        assert(t.hasDatabasePeer());
        buf.resize(0);
        buf += "DROP TABLE IF EXISTS ";
        buf += getTableName(t.tname);
        buf += ";";
        return performQuery(buf);
    }

    virtual void optimizePeerTableOnRemote(const SQLTable &) { }

    bool performCreatePeerTable(
        RandomGenerator & rg,
        const bool is_clickhouse_integration,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries)
    {
        //drop table if exists in other db
        bool res = dropPeerTableOnRemote(t);

        //create table on other server
        if (res && is_clickhouse_integration)
        {
            CreateTable newt;
            newt.CopyFrom(*ct);

            assert(newt.has_est() && !newt.has_table_as());
            ExprSchemaTable & est = const_cast<ExprSchemaTable &>(newt.est());
            est.mutable_database()->set_database("test");

            buf.resize(0);
            CreateTableToString(buf, newt);
            buf += ";";
            res &= performQuery(buf);
        }
        else if (res)
        {
            res &= performIntegration(rg, t.tname, false, entries);
        }
        return res;
    }

    void virtual truncateStatement(std::string &) { }

    void truncatePeerTableOnRemote(const SQLTable & t)
    {
        assert(t.hasDatabasePeer());
        buf.resize(0);
        truncateStatement(buf);
        buf += " ";
        buf += getTableName(t.tname);
        buf += ";";
        (void)performQuery(buf);
    }

    ~ClickHouseIntegratedDatabase() override = default;
};

class MySQLIntegration : public ClickHouseIntegratedDatabase
{
#if defined USE_MYSQL && USE_MYSQL
private:
    const bool is_clickhouse = false;
    MYSQL * mysql_connection = nullptr;

public:
    MySQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, const bool is_click, MYSQL * mcon)
        : ClickHouseIntegratedDatabase(fcc, scc), is_clickhouse(is_click), mysql_connection(mcon)
    {
    }

    static MySQLIntegration *
    TestAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log, const std::string & server)
    {
        MYSQL * mcon = nullptr;

        if (!(mcon = mysql_init(nullptr)))
        {
            LOG_ERROR(fcc.log, "Could not initialize MySQL handle");
        }
        else if (!mysql_real_connect(
                     mcon,
                     scc.hostname.empty() ? nullptr : scc.hostname.c_str(),
                     scc.user.empty() ? nullptr : scc.user.c_str(),
                     scc.password.empty() ? nullptr : scc.password.c_str(),
                     nullptr,
                     scc.mysql_port ? scc.mysql_port : scc.port,
                     scc.unix_socket.empty() ? nullptr : scc.unix_socket.c_str(),
                     0))
        {
            LOG_ERROR(fcc.log, "{} connection error: {}", server, mysql_error(mcon));
            mysql_close(mcon);
        }
        else
        {
            MySQLIntegration * mysql = new MySQLIntegration(fcc, scc, server == "ClickHouse", mcon);

            if (read_log
                || (mysql->performQuery("DROP DATABASE IF EXISTS " + scc.database + ";")
                    && mysql->performQuery("CREATE DATABASE " + scc.database + ";")))
            {
                LOG_INFO(fcc.log, "Connected to {}", server);
                return mysql;
            }
            else
            {
                delete mysql;
            }
        }
        return nullptr;
    }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const std::string & tname, TableEngine * te) override
    {
        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.mysql_port ? sc.mysql_port : sc.port));
        te->add_params()->set_svalue(sc.database);
        te->add_params()->set_svalue(tname);
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
        if (rg.nextBool())
        {
            te->add_params()->set_num(rg.nextBool() ? 1 : 0);
        }
    }

    std::string getTableName(const uint32_t tname) override { return "test.t" + std::to_string(tname); }

    void truncateStatement(std::string & outbuf) override
    {
        outbuf += "TRUNCATE";
        if (is_clickhouse)
        {
            outbuf += " TABLE";
        }
    }

    void optimizePeerTableOnRemote(const SQLTable & t) override
    {
        assert(t.hasDatabasePeer());
        if (is_clickhouse && t.supportsFinal())
        {
            buf.resize(0);
            buf += "OPTIMIZE TABLE ";
            buf += getTableName(t.tname);
            buf += " FINAL;";
            (void)performQuery(buf);
        }
    }

    bool performQuery(const std::string & query) override
    {
        if (!mysql_connection)
        {
            LOG_ERROR(fc.log, "Not connected to MySQL");
            return false;
        }
        out_file << query << std::endl;
        if (mysql_query(mysql_connection, query.c_str()))
        {
            LOG_ERROR(fc.log, "MySQL query: {} Error: {}", query, mysql_error(mysql_connection));
            return false;
        }
        return true;
    }

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, std::string & out) const override { tp->MySQLtypeName(rg, out, false); }

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

    static MySQLIntegration * TestAndAddMySQLConnection(const FuzzConfig & fcc, const ServerCredentials &, const bool, const std::string &)
    {
        LOG_INFO(fcc.log, "ClickHouse not compiled with MySQL connector, skipping MySQL integration");
        return nullptr;
    }

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
    TestAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log)
    {
        bool has_something = false;
        std::string connection_str;
        pqxx::connection * pcon = nullptr;
        PostgreSQLIntegration * psql = nullptr;

        if (!scc.unix_socket.empty() || !scc.hostname.empty())
        {
            connection_str += "host='";
            connection_str += scc.unix_socket.empty() ? scc.hostname : scc.unix_socket;
            connection_str += "'";
            has_something = true;
        }
        if (scc.port)
        {
            if (has_something)
            {
                connection_str += " ";
            }
            connection_str += "port='";
            connection_str += std::to_string(scc.port);
            connection_str += "'";
            has_something = true;
        }
        if (!scc.user.empty())
        {
            if (has_something)
            {
                connection_str += " ";
            }
            connection_str += "user='";
            connection_str += scc.user;
            connection_str += "'";
            has_something = true;
        }
        if (!scc.password.empty())
        {
            if (has_something)
            {
                connection_str += " ";
            }
            connection_str += "password='";
            connection_str += scc.password;
            connection_str += "'";
        }
        if (!scc.database.empty())
        {
            if (has_something)
            {
                connection_str += " ";
            }
            connection_str += "dbname='";
            connection_str += scc.database;
            connection_str += "'";
        }
        try
        {
            if (!(pcon = new pqxx::connection(connection_str)))
            {
                LOG_ERROR(fcc.log, "Could not initialize PostgreSQL handle");
            }
            else
            {
                psql = new PostgreSQLIntegration(fcc, scc, pcon);
                if (read_log || (psql->performQuery("DROP SCHEMA IF EXISTS test CASCADE;") && psql->performQuery("CREATE SCHEMA test;")))
                {
                    LOG_INFO(fcc.log, "Connected to PostgreSQL");
                    return psql;
                }
            }
        }
        catch (std::exception const & e)
        {
            LOG_ERROR(fcc.log, "PostgreSQL connection error: {}", e.what());
        }
        delete psql;
        delete pcon;
        return nullptr;
    }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const std::string & tname, TableEngine * te) override
    {
        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_svalue(sc.database);
        te->add_params()->set_svalue(tname);
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
        te->add_params()->set_svalue("test");
        if (rg.nextSmallNumber() < 4)
        {
            te->add_params()->set_svalue("ON CONFLICT DO NOTHING");
        }
    }

    std::string getTableName(const uint32_t tname) override { return "test.t" + std::to_string(tname); }

    void truncateStatement(std::string & outbuf) override { outbuf += "TRUNCATE"; }

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, std::string & out) const override
    {
        tp->PostgreSQLtypeName(rg, out, false);
    }

    bool performQuery(const std::string & query) override
    {
        if (!postgres_connection)
        {
            LOG_ERROR(fc.log, "Not connected to PostgreSQL");
            return false;
        }
        try
        {
            pqxx::work w(*postgres_connection);

            out_file << query << std::endl;
            (void)w.exec(query);
            w.commit();
            return true;
        }
        catch (std::exception const & e)
        {
            LOG_ERROR(fc.log, "PostgreSQL query: {} Error: {}", query, e.what());
            return false;
        }
    }

    ~PostgreSQLIntegration() override { delete postgres_connection; }
#else
public:
    PostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static PostgreSQLIntegration * TestAndAddPostgreSQLIntegration(const FuzzConfig & fcc, const ServerCredentials &, const bool)
    {
        LOG_INFO(fcc.log, "ClickHouse not compiled with PostgreSQL connector, skipping PostgreSQL integration");
        return nullptr;
    }

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

    static SQLiteIntegration * TestAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc)
    {
        sqlite3 * scon = nullptr;
        const std::filesystem::path spath = fcc.db_file_path / "sqlite.db";

        if (sqlite3_open(spath.c_str(), &scon) != SQLITE_OK)
        {
            if (scon)
            {
                LOG_ERROR(fcc.log, "SQLite connection error: {}", sqlite3_errmsg(scon));
                sqlite3_close(scon);
            }
            else
            {
                LOG_ERROR(fcc.log, "Could not initialize SQLite handle");
            }
            return nullptr;
        }
        else
        {
            LOG_INFO(fcc.log, "Connected to SQLite");
            return new SQLiteIntegration(fcc, scc, scon, spath);
        }
    }

    void setEngineDetails(RandomGenerator &, const SQLBase &, const std::string & tname, TableEngine * te) override
    {
        te->add_params()->set_svalue(sqlite_path.generic_string());
        te->add_params()->set_svalue(tname);
    }

    std::string getTableName(const uint32_t tname) override { return "t" + std::to_string(tname); }

    void truncateStatement(std::string & outbuf) override { outbuf += "DELETE FROM"; }

    void columnTypeAsString(RandomGenerator & rg, SQLType * tp, std::string & out) const override { tp->SQLitetypeName(rg, out, false); }

    bool performQuery(const std::string & query) override
    {
        char * err_msg = nullptr;

        if (!sqlite_connection)
        {
            LOG_ERROR(fc.log, "Not connected to SQLite");
            return false;
        }
        out_file << query << std::endl;
        if (sqlite3_exec(sqlite_connection, query.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
        {
            LOG_ERROR(fc.log, "SQLite query: {} Error: {}", query, err_msg);
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
#else
public:
    const std::filesystem::path sqlite_path;

    SQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegratedDatabase(fcc, scc) { }

    static SQLiteIntegration * TestAndAddSQLiteIntegration(const FuzzConfig & fcc, const ServerCredentials &)
    {
        LOG_INFO(fcc.log, "ClickHouse not compiled with SQLite connector, skipping SQLite integration");
        return nullptr;
    }

    ~SQLiteIntegration() override = default;
#endif
};

class RedisIntegration : public ClickHouseIntegration
{
public:
    RedisIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    void setEngineDetails(RandomGenerator & rg, const SQLBase &, const std::string &, TableEngine * te) override
    {
        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_num(rg.nextBool() ? 0 : rg.nextLargeNumber() % 16);
        te->add_params()->set_svalue(sc.password);
        te->add_params()->set_num(rg.nextBool() ? 16 : rg.nextLargeNumber() % 33);
    }

    bool performIntegration(RandomGenerator &, const uint32_t, const bool, std::vector<ColumnPathChain> &) override { return true; }

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
    void documentAppendBottomType(RandomGenerator & rg, const std::string & cname, T & output, SQLType * tp);

    void
    documentAppendArray(RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, ArrayType * at);
    void
    documentAppendAnyValue(RandomGenerator & rg, const std::string & cname, bsoncxx::builder::stream::document & document, SQLType * tp);

public:
    MongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc, mongocxx::client & mcon, mongocxx::database & db)
        : ClickHouseIntegration(fcc, scc)
        , out_file(std::ofstream(scc.query_log_file, std::ios::out | std::ios::trunc))
        , client(std::move(mcon))
        , database(std::move(db))
    {
    }

    static MongoDBIntegration * TestAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc)
    {
        std::string connection_str = "mongodb://";

        if (!scc.user.empty())
        {
            connection_str += scc.user;
            if (!scc.password.empty())
            {
                connection_str += ":";
                connection_str += scc.password;
            }
            connection_str += "@";
        }
        connection_str += scc.hostname;
        connection_str += ":";
        connection_str += std::to_string(scc.port);

        try
        {
            bool db_exists = false;
            mongocxx::client client = mongocxx::client(mongocxx::uri(std::move(connection_str)));
            auto databases = client.list_databases();

            for (const auto & db : databases)
            {
                if (db["name"].get_utf8().value == scc.database)
                {
                    db_exists = true;
                    break;
                }
            }

            if (db_exists)
            {
                client[scc.database].drop();
            }

            mongocxx::database db = client[scc.database];
            db.create_collection("test");

            LOG_INFO(fcc.log, "Connected to MongoDB");
            return new MongoDBIntegration(fcc, scc, client, db);
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(fcc.log, "MongoDB connection error: {}", e.what());
            return nullptr;
        }
    }

    void setEngineDetails(RandomGenerator &, const SQLBase &, const std::string & tname, TableEngine * te) override
    {
        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_svalue(sc.database);
        te->add_params()->set_svalue(tname);
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
    }

    bool
    performIntegration(RandomGenerator & rg, const uint32_t tname, const bool can_shuffle, std::vector<ColumnPathChain> & entries) override
    {
        try
        {
            const bool permute = can_shuffle && rg.nextBool();
            const bool miss_cols = rg.nextBool();
            const uint32_t ndocuments = rg.nextMediumNumber();
            const std::string str_tname = "t" + std::to_string(tname);
            mongocxx::collection coll = database[str_tname];

            for (uint32_t j = 0; j < ndocuments; j++)
            {
                bsoncxx::builder::stream::document document{};

                if (permute && rg.nextSmallNumber() < 4)
                {
                    std::shuffle(entries.begin(), entries.end(), rg.generator);
                }
                for (const auto & entry : entries)
                {
                    if (miss_cols && rg.nextSmallNumber() < 4)
                    {
                        //sometimes the column is missing
                        documentAppendAnyValue(rg, entry.getBottomName(), document, entry.getBottomType());
                        assert(entry.path.size() == 1);
                    }
                }
                documents.push_back(document << bsoncxx::builder::stream::finalize);
            }
            out_file << str_tname << std::endl; //collection name
            for (const auto & doc : documents)
            {
                out_file << bsoncxx::to_json(doc.view()) << std::endl; // Write each JSON document on a new line
            }
            coll.insert_many(documents);
            documents.clear();
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(fc.log, "MongoDB connection error: {}", e.what());
            return false;
        }
        return true;
    }

    ~MongoDBIntegration() override = default;
#else
public:
    MongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials & scc) : ClickHouseIntegration(fcc, scc) { }

    static MongoDBIntegration * TestAndAddMongoDBIntegration(const FuzzConfig & fcc, const ServerCredentials &)
    {
        LOG_INFO(fcc.log, "ClickHouse not compiled with MongoDB connector, skipping MongoDB integration");
        return nullptr;
    }

    ~MongoDBIntegration() override = default;
#endif
};

class MinIOIntegration : public ClickHouseIntegration
{
private:
    bool sendRequest(const std::string & resource);

public:
    explicit MinIOIntegration(const FuzzConfig & fcc, const ServerCredentials & ssc) : ClickHouseIntegration(fcc, ssc) { }

    void setEngineDetails(RandomGenerator &, const SQLBase & b, const std::string & tname, TableEngine * te) override
    {
        te->add_params()->set_svalue(
            "http://" + sc.hostname + ":" + std::to_string(sc.port) + sc.database + "/file" + tname.substr(1)
            + (b.isS3QueueEngine() ? "/*" : ""));
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
    }

    bool performIntegration(RandomGenerator &, const uint32_t tname, const bool, std::vector<ColumnPathChain> &) override
    {
        return sendRequest(sc.database + "/file" + std::to_string(tname));
    }

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

    explicit ExternalIntegrations(const FuzzConfig & fc)
    {
        if (fc.mysql_server.has_value())
        {
            mysql = MySQLIntegration::TestAndAddMySQLConnection(fc, fc.mysql_server.value(), fc.read_log, "MySQL");
        }
        if (fc.postgresql_server.has_value())
        {
            postresql = PostgreSQLIntegration::TestAndAddPostgreSQLIntegration(fc, fc.postgresql_server.value(), fc.read_log);
        }
        if (fc.sqlite_server.has_value())
        {
            sqlite = SQLiteIntegration::TestAndAddSQLiteIntegration(fc, fc.sqlite_server.value());
        }
        if (fc.mongodb_server.has_value())
        {
            mongodb = MongoDBIntegration::TestAndAddMongoDBIntegration(fc, fc.mongodb_server.value());
        }
        if (fc.redis_server.has_value())
        {
            redis = new RedisIntegration(fc, fc.redis_server.value());
        }
        if (fc.minio_server.has_value())
        {
            minio = new MinIOIntegration(fc, fc.minio_server.value());
        }
        if (fc.clickhouse_server.has_value())
        {
            clickhouse = MySQLIntegration::TestAndAddMySQLConnection(fc, fc.clickhouse_server.value(), fc.read_log, "ClickHouse");
        }
    }

    void createExternalDatabaseTable(
        RandomGenerator & rg, const IntegrationCall dc, const SQLBase & b, std::vector<ColumnPathChain> & entries, TableEngine * te)
    {
        const std::string & tname = "t" + std::to_string(b.tname);

        requires_external_call_check++;
        switch (dc)
        {
            case IntegrationCall::MySQL:
                next_calls_succeeded.push_back(mysql->performIntegration(rg, b.tname, true, entries));
                mysql->setEngineDetails(rg, b, tname, te);
                break;
            case IntegrationCall::PostgreSQL:
                next_calls_succeeded.push_back(postresql->performIntegration(rg, b.tname, true, entries));
                postresql->setEngineDetails(rg, b, tname, te);
                break;
            case IntegrationCall::SQLite:
                next_calls_succeeded.push_back(sqlite->performIntegration(rg, b.tname, true, entries));
                sqlite->setEngineDetails(rg, b, tname, te);
                break;
            case IntegrationCall::MongoDB:
                next_calls_succeeded.push_back(mongodb->performIntegration(rg, b.tname, true, entries));
                mongodb->setEngineDetails(rg, b, tname, te);
                break;
            case IntegrationCall::Redis:
                next_calls_succeeded.push_back(redis->performIntegration(rg, b.tname, true, entries));
                redis->setEngineDetails(rg, b, tname, te);
                break;
            case IntegrationCall::MinIO:
                next_calls_succeeded.push_back(minio->performIntegration(rg, b.tname, true, entries));
                minio->setEngineDetails(rg, b, tname, te);
                break;
        }
    }

    void createPeerTable(
        RandomGenerator & rg,
        const PeerTableDatabase pt,
        const SQLTable & t,
        const CreateTable * ct,
        std::vector<ColumnPathChain> & entries)
    {
        requires_external_call_check++;
        switch (pt)
        {
            case PeerTableDatabase::ClickHouse:
                next_calls_succeeded.push_back(clickhouse->performCreatePeerTable(rg, true, t, ct, entries));
                break;
            case PeerTableDatabase::MySQL:
                next_calls_succeeded.push_back(mysql->performCreatePeerTable(rg, false, t, ct, entries));
                break;
            case PeerTableDatabase::PostgreSQL:
                next_calls_succeeded.push_back(postresql->performCreatePeerTable(rg, false, t, ct, entries));
                break;
            case PeerTableDatabase::SQLite:
                next_calls_succeeded.push_back(sqlite->performCreatePeerTable(rg, false, t, ct, entries));
                break;
            case PeerTableDatabase::None:
                assert(0);
                break;
        }
    }

    void truncatePeerTableOnRemote(const SQLTable & t)
    {
        switch (t.peer_table)
        {
            case PeerTableDatabase::ClickHouse:
                clickhouse->truncatePeerTableOnRemote(t);
                break;
            case PeerTableDatabase::MySQL:
                mysql->truncatePeerTableOnRemote(t);
                break;
            case PeerTableDatabase::PostgreSQL:
                postresql->truncatePeerTableOnRemote(t);
                break;
            case PeerTableDatabase::SQLite:
                sqlite->truncatePeerTableOnRemote(t);
                break;
            case PeerTableDatabase::None:
                break;
        }
    }

    void optimizePeerTableOnRemote(const SQLTable & t)
    {
        switch (t.peer_table)
        {
            case PeerTableDatabase::ClickHouse:
                clickhouse->optimizePeerTableOnRemote(t);
                break;
            case PeerTableDatabase::MySQL:
            case PeerTableDatabase::PostgreSQL:
            case PeerTableDatabase::SQLite:
            case PeerTableDatabase::None:
                break;
        }
    }

    void dropPeerTableOnRemote(const SQLTable & t)
    {
        switch (t.peer_table)
        {
            case PeerTableDatabase::ClickHouse:
                clickhouse->dropPeerTableOnRemote(t);
                break;
            case PeerTableDatabase::MySQL:
                mysql->dropPeerTableOnRemote(t);
                break;
            case PeerTableDatabase::PostgreSQL:
                postresql->dropPeerTableOnRemote(t);
                break;
            case PeerTableDatabase::SQLite:
                sqlite->dropPeerTableOnRemote(t);
                break;
            case PeerTableDatabase::None:
                break;
        }
    }

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
