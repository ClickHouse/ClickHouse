#include <cinttypes>
#include <cstring>
#include <ctime>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <Client/BuzzHouse/Generator/ExternalIntegrations.h>
#include <Client/BuzzHouse/Generator/RandomSettings.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

#include <IO/copyData.h>
#include <base/scope_guard.h>
#include <Common/ShellCommand.h>

namespace BuzzHouse
{

bool ClickHouseIntegratedDatabase::performIntegration(
    RandomGenerator & rg,
    std::shared_ptr<SQLDatabase> db,
    const uint32_t tname,
    const bool can_shuffle,
    const bool is_deterministic,
    std::vector<ColumnPathChain> & entries)
{
    const String str_tname = getTableName(db, tname);

    if (performQuery(fmt::format("DROP TABLE IF EXISTS {};", str_tname)))
    {
        String buf;
        bool first = true;

        if (can_shuffle && rg.nextSmallNumber() < 7)
        {
            std::shuffle(entries.begin(), entries.end(), rg.generator);
        }
        for (const auto & entry : entries)
        {
            SQLType * tp = entry.getBottomType();

            buf += fmt::format(
                "{}{} {} {}NULL",
                first ? "" : ", ",
                entry.getBottomName(),
                columnTypeAsString(rg, is_deterministic, tp),
                ((entry.nullable.has_value() && entry.nullable.value()) || hasType<Nullable>(false, false, false, tp)) ? "" : "NOT ");
            first = false;
        }
        return performQuery(fmt::format("CREATE TABLE {}({});", str_tname, buf));
    }
    return false;
}

bool ClickHouseIntegratedDatabase::dropPeerTableOnRemote(const SQLTable & t)
{
    chassert(t.hasDatabasePeer());
    return performQuery(fmt::format("DROP TABLE IF EXISTS {};", getTableName(t.db, t.tname)));
}

void ClickHouseIntegratedDatabase::swapTableDefinitions(RandomGenerator & rg, CreateTable & newt)
{
    TableEngine & te = const_cast<TableEngine &>(newt.engine());
    const auto & teng = te.engine();

    if (te.has_setting_values() && rg.nextSmallNumber() < 10)
    {
        /// Swap table settings
        const auto & allSettings = allTableSettings.at(teng);
        const auto & svs = te.setting_values();

        for (int i = 0; i < svs.other_values_size() + 1; i++)
        {
            SetValue & sv = const_cast<SetValue &>(i == 0 ? svs.set_value() : svs.other_values(i - 1));

            if (allSettings.find(sv.property()) != allSettings.end())
            {
                const CHSetting & chs = allSettings.at(sv.property());

                if (!chs.changes_behavior && !chs.oracle_values.empty() && rg.nextSmallNumber() < 8)
                {
                    if (chs.oracle_values.size() == 2)
                    {
                        const String & fval = *chs.oracle_values.begin();

                        sv.set_value(sv.value() == fval ? *std::next(chs.oracle_values.begin(), 1) : fval);
                    }
                    else
                    {
                        sv.set_value(rg.pickRandomly(chs.oracle_values));
                    }
                }
            }
        }
    }
    if (teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree)
    {
        if (te.has_partition_by() && rg.nextSmallNumber() < 5)
        {
            /// Remove partition by
            te.clear_partition_by();
        }
        if (te.has_primary_key() && te.has_order() && rg.nextSmallNumber() < 5)
        {
            /// Remove primary key or order by clause
            if (rg.nextBool())
            {
                te.clear_primary_key();
            }
            else
            {
                te.clear_order();
            }
        }
        if (te.has_order())
        {
            /// Swap ASC/DESC
            for (int i = 0; i < te.order().exprs_size(); i++)
            {
                if (rg.nextSmallNumber() < 9)
                {
                    TableKeyExpr & tke = const_cast<TableKeyExpr &>(te.order().exprs(i));

                    tke.set_asc_desc((!tke.has_asc_desc() || tke.asc_desc() == AscDesc::ASC) ? AscDesc::DESC : AscDesc::ASC);
                }
            }
        }
    }
    else if (teng >= TableEngineValues::StripeLog && teng <= TableEngineValues::TinyLog && rg.nextSmallNumber() < 5)
    {
        /// Swap engine if others are equivalent
        static const std::vector<TableEngineValues> & logEngines
            = {TableEngineValues::StripeLog, TableEngineValues::Log, TableEngineValues::TinyLog};

        te.set_engine(rg.pickRandomly(logEngines));
    }
    if (newt.has_table_def())
    {
        const TableDef & def = newt.table_def();

        for (int i = 0; i < def.other_defs_size() + 1; i++)
        {
            if (i == 0 || def.other_defs(i - 1).has_col_def())
            {
                ColumnDef & cdef = const_cast<ColumnDef &>(i == 0 ? def.col_def() : def.other_defs(i - 1).col_def());
                TopTypeName & ttn = const_cast<TopTypeName &>(cdef.type().type());

                if (cdef.has_codecs() && rg.nextBool())
                {
                    /// Clear codecs
                    cdef.clear_codecs();
                }
                if (cdef.has_stats() && rg.nextBool())
                {
                    /// Clear statistics
                    cdef.clear_stats();
                }
                /// Remove LowCardinality property
                if (ttn.has_nullable_lcard() && rg.nextBool())
                {
                    ttn.set_allocated_nullable(ttn.release_nullable_lcard());
                }
                else if (ttn.has_non_nullable_lcard() && rg.nextBool())
                {
                    ttn.set_allocated_non_nullable(ttn.release_non_nullable_lcard());
                }
                if (cdef.has_setting_values())
                {
                    if (rg.nextBool())
                    {
                        /// Clear all settings, so far none changes behavior
                        cdef.clear_setting_values();
                    }
                    else
                    {
                        const auto & allSettings = allTableSettings.at(teng);
                        const auto & svs = cdef.setting_values();

                        for (int j = 0; j < svs.other_values_size() + 1; j++)
                        {
                            SetValue & sv = const_cast<SetValue &>(j == 0 ? svs.set_value() : svs.other_values(j - 1));

                            if (allSettings.find(sv.property()) != allSettings.end())
                            {
                                const CHSetting & chs = allSettings.at(sv.property());

                                chassert(!chs.changes_behavior);
                                if (!chs.oracle_values.empty() && rg.nextSmallNumber() < 8)
                                {
                                    sv.set_value(rg.pickRandomly(chs.oracle_values));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if (newt.has_cluster() && rg.nextSmallNumber() < 4)
    {
        newt.clear_cluster();
    }
    else if (!fc.clusters.empty() && rg.nextSmallNumber() < 4)
    {
        newt.clear_cluster();
        newt.mutable_cluster()->set_cluster(rg.pickRandomly(fc.clusters));
    }
}

bool ClickHouseIntegratedDatabase::performCreatePeerTable(
    RandomGenerator & rg,
    const bool is_clickhouse_integration,
    const SQLTable & t,
    const CreateTable * ct,
    std::vector<ColumnPathChain> & entries)
{
    /// Drop table if exists in other db
    bool res = dropPeerTableOnRemote(t);

    /// Create table on other db
    if (res && is_clickhouse_integration)
    {
        if (t.db)
        {
            String buf;
            CreateDatabase newd;
            DatabaseEngine * deng = newd.mutable_dengine();

            newd.set_if_not_exists(true);
            deng->set_engine(t.db->deng);
            t.db->setName(newd.mutable_database());
            t.db->finishDatabaseSpecification(deng);
            CreateDatabaseToString(buf, newd);
            res &= performQuery(buf + ";");
        }
        if (res)
        {
            String buf;
            CreateTable newt;
            newt.CopyFrom(*ct);

            chassert(newt.has_est() && !newt.has_table_as());
            ExprSchemaTable & est = const_cast<ExprSchemaTable &>(newt.est());
            if (t.db)
            {
                t.db->setName(est.mutable_database());
            }
            if (rg.nextMediumNumber() < 91)
            {
                this->swapTableDefinitions(rg, newt);
            }

            CreateTableToString(buf, newt);
            res &= performQuery(buf + ";");
        }
    }
    else if (res)
    {
        res &= performIntegration(rg, is_clickhouse_integration ? t.db : nullptr, t.tname, false, t.is_deterministic, entries);
    }
    return res;
}

bool ClickHouseIntegratedDatabase::truncatePeerTableOnRemote(const SQLTable & t)
{
    chassert(t.hasDatabasePeer());
    return performQuery(fmt::format("{} {};", truncateStatement(), getTableName(t.db, t.tname)));
}

bool ClickHouseIntegratedDatabase::performQueryOnServerOrRemote(const PeerTableDatabase pt, const String & query)
{
    switch (pt)
    {
        case PeerTableDatabase::ClickHouse:
        case PeerTableDatabase::MySQL:
        case PeerTableDatabase::PostgreSQL:
        case PeerTableDatabase::SQLite:
            return performQuery(query);
        case PeerTableDatabase::None:
            return fc.processServerQuery(false, query);
    }
}

#if defined USE_MYSQL && USE_MYSQL
void MySQLIntegration::closeMySQLConnection(MYSQL * mysql)
{
    if (mysql)
    {
        mysql_close(mysql);
    }
}

std::unique_ptr<MySQLIntegration>
MySQLIntegration::testAndAddMySQLConnection(FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log, const String & server)
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
        std::unique_ptr<MySQLIntegration> mysql
            = std::make_unique<MySQLIntegration>(fcc, scc, server == "ClickHouse", MySQLUniqueKeyPtr(mcon, closeMySQLConnection));

        if (read_log
            || (mysql->performQuery("DROP DATABASE IF EXISTS " + scc.database + ";")
                && mysql->performQuery("CREATE DATABASE " + scc.database + ";")))
        {
            LOG_INFO(fcc.log, "Connected to {}", server);
            return mysql;
        }
    }
    return nullptr;
}

void MySQLIntegration::setEngineDetails(RandomGenerator & rg, const SQLBase & b, const String & tname, TableEngine * te)
{
    if (b.isExternalDistributedEngine())
    {
        te->add_params()->set_svalue("MySQL");
    }
    te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.mysql_port ? sc.mysql_port : sc.port));
    te->add_params()->set_svalue(sc.database);
    te->add_params()->set_svalue(tname);
    te->add_params()->set_svalue(sc.user);
    te->add_params()->set_svalue(sc.password);
    if (!b.isExternalDistributedEngine() && rg.nextBool())
    {
        te->add_params()->set_num(rg.nextBool() ? 1 : 0);
    }
}

String MySQLIntegration::getTableName(std::shared_ptr<SQLDatabase> db, const uint32_t tname)
{
    const auto prefix = is_clickhouse ? (db ? fmt::format("d{}.", db->dname) : "") : "test.";
    return fmt::format("{}t{}", prefix, tname);
}

String MySQLIntegration::truncateStatement()
{
    return fmt::format("TRUNCATE{}", is_clickhouse ? " TABLE" : "");
}

bool MySQLIntegration::optimizeTableForOracle(const PeerTableDatabase pt, const SQLTable & t)
{
    chassert(t.hasDatabasePeer());
    if (is_clickhouse && t.isMergeTreeFamily())
    {
        /// Sometimes the optimize step doesn't have to do anything, then throws error. Ignore it
        const auto u = performQueryOnServerOrRemote(pt, fmt::format("ALTER TABLE {} APPLY DELETED MASK;", getTableName(t.db, t.tname)));
        const auto v = performQueryOnServerOrRemote(
            pt, fmt::format("OPTIMIZE TABLE {}{};", getTableName(t.db, t.tname), t.supportsFinal() ? " FINAL" : ""));
        UNUSED(u);
        UNUSED(v);
    }
    return true;
}

bool MySQLIntegration::performQuery(const String & query)
{
    if (!mysql_connection)
    {
        LOG_ERROR(fc.log, "Not connected to MySQL");
        return false;
    }
    out_file << query << std::endl;
    if (mysql_query(mysql_connection.get(), query.c_str()))
    {
        LOG_ERROR(fc.log, "MySQL query: {} Error: {}", query, mysql_error(mysql_connection.get()));
        return false;
    }
    else
    {
        MYSQL_RES * result = mysql_store_result(mysql_connection.get());

        while (mysql_fetch_row(result))
            ;
        mysql_free_result(result);
    }
    return true;
}

String MySQLIntegration::columnTypeAsString(RandomGenerator & rg, const bool is_deterministic, SQLType * tp) const
{
    if (!is_deterministic && rg.nextSmallNumber() < 4)
    {
        /// Use a random MySQL type
        const uint32_t nopt = rg.nextMediumNumber();

        if (nopt < 76)
        {
            static const std::vector<String> & baseTypes
                = {"TINYINT",
                   "SMALLINT",
                   "MEDIUMINT",
                   "INT",
                   "INTEGER",
                   "BIGINT",
                   "TINYINT UNSIGNED",
                   "SMALLINT UNSIGNED",
                   "MEDIUMINT UNSIGNED",
                   "INT UNSIGNED",
                   "INTEGER UNSIGNED",
                   "BIGINT UNSIGNED",
                   "SERIAL",
                   "FLOAT",
                   "REAL",
                   "DOUBLE",
                   "DOUBLE PRECISION",
                   "FIXED",
                   "DEC",
                   "DECIMAL",
                   "NUMERIC",
                   "TINYBLOB",
                   "BLOB",
                   "MEDIUMBLOB",
                   "LONGBLOB",
                   "TINYTEXT",
                   "TEXT",
                   "MEDIUMTEXT",
                   "LONGTEXT",
                   "DATE",
                   "TIME",
                   "DATETIME",
                   "TIMESTAMP",
                   "YEAR",
                   "GEOMETRY",
                   "POINT",
                   "LINESTRING",
                   "POLYGON",
                   "MULTIPOINT",
                   "MULTILINESTRING",
                   "MULTIPOLYGON",
                   "GEOMETRYCOLLECTION",
                   "JSON",
                   "BOOL",
                   "BOOLEAN"};
            return rg.pickRandomly(baseTypes);
        }
        else if (nopt < 81)
        {
            /// Bit type
            std::uniform_int_distribution<uint32_t> lengths(1, 64);

            return fmt::format("BIT({})", lengths(rg.generator));
        }
        else if (nopt < 86)
        {
            /// Decimal/Numeric
            std::uniform_int_distribution<uint32_t> precisions(0, 65);
            const uint32_t precision = precisions(rg.generator);
            std::uniform_int_distribution<uint32_t> scales(UINT32_C(0), std::min(UINT32_C(30), precision));
            static const std::vector<String> & baseTypes = {"FIXED", "DEC", "DECIMAL", "NUMERIC"};

            return fmt::format("{}({},{})", rg.pickRandomly(baseTypes), precision, scales(rg.generator));
        }
        else if (nopt < 91)
        {
            /// Character types
            std::uniform_int_distribution<uint32_t> lengths(1, 65535);
            static const std::vector<String> & baseTypes = {"CHAR", "VARCHAR", "BINARY", "VARBINARY"};

            return fmt::format("{}({})", rg.pickRandomly(baseTypes), lengths(rg.generator));
        }
        else if (nopt < 96)
        {
            /// Date/time with precision
            std::uniform_int_distribution<uint32_t> precisions(0, 6);
            static const std::vector<String> & baseTypes = {"TIME", "TIMESTAMP", "DATETIME"};

            return fmt::format("{}({})", rg.pickRandomly(baseTypes), precisions(rg.generator));
        }
        else
        {
            /// Set/enum types
            String desc;
            std::uniform_int_distribution<uint32_t> number_values(1, 64);
            const uint32_t nvalues = number_values(rg.generator);

            for (uint32_t i = 0; i < nvalues; i++)
            {
                if (i > 0)
                {
                    desc += ", ";
                }
                desc += "'value_" + std::to_string(i + 1) + "'";
            }
            return fmt::format("{}({})", rg.nextBool() ? "ENUM" : "SET", desc);
        }
    }
    return tp->MySQLtypeName(rg, false);
}
#else
std::unique_ptr<MySQLIntegration>
MySQLIntegration::testAndAddMySQLConnection(FuzzConfig & fcc, const ServerCredentials &, const bool, const String &)
{
    LOG_INFO(fcc.log, "ClickHouse not compiled with MySQL connector, skipping MySQL integration");
    return nullptr;
}
#endif

#if defined USE_LIBPQXX && USE_LIBPQXX
void PostgreSQLIntegration::closePostgreSQLConnection(pqxx::connection * psql)
{
    if (psql)
    {
        psql->close();
    }
}

std::unique_ptr<PostgreSQLIntegration>
PostgreSQLIntegration::testAndAddPostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials & scc, const bool read_log)
{
    String connection_str;
    bool has_something = false;

    if (!scc.unix_socket.empty() || !scc.hostname.empty())
    {
        connection_str += fmt::format("host='{}'", scc.unix_socket.empty() ? scc.hostname : scc.unix_socket);
        has_something = true;
    }
    if (scc.port)
    {
        connection_str += fmt::format("{}port='{}'", has_something ? " " : "", scc.port);
        has_something = true;
    }
    if (!scc.user.empty())
    {
        connection_str += fmt::format("{}user='{}'", has_something ? " " : "", scc.user);
        has_something = true;
    }
    if (!scc.password.empty())
    {
        connection_str += fmt::format("{}password='{}'", has_something ? " " : "", scc.password);
    }
    if (!scc.database.empty())
    {
        connection_str += fmt::format("{}dbname='{}'", has_something ? " " : "", scc.database);
    }
    try
    {
        std::unique_ptr<PostgreSQLIntegration> psql = std::make_unique<PostgreSQLIntegration>(
            fcc, scc, PostgreSQLUniqueKeyPtr(new pqxx::connection(connection_str), closePostgreSQLConnection));

        if (read_log || (psql->performQuery("DROP SCHEMA IF EXISTS test CASCADE;") && psql->performQuery("CREATE SCHEMA test;")))
        {
            LOG_INFO(fcc.log, "Connected to PostgreSQL");
            return psql;
        }
    }
    catch (std::exception const & e)
    {
        LOG_ERROR(fcc.log, "PostgreSQL connection error: {}", e.what());
    }
    return nullptr;
}

void PostgreSQLIntegration::setEngineDetails(RandomGenerator & rg, const SQLBase & b, const String & tname, TableEngine * te)
{
    if (b.isExternalDistributedEngine())
    {
        te->add_params()->set_svalue("PostgreSQL");
    }
    te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
    te->add_params()->set_svalue(sc.database);
    te->add_params()->set_svalue(tname);
    te->add_params()->set_svalue(sc.user);
    te->add_params()->set_svalue(sc.password);
    te->add_params()->set_svalue("test");
    if (!b.isExternalDistributedEngine() && !b.isMaterializedPostgreSQLEngine() && rg.nextSmallNumber() < 4)
    {
        te->add_params()->set_svalue("ON CONFLICT DO NOTHING");
    }
}

String PostgreSQLIntegration::getTableName(std::shared_ptr<SQLDatabase>, const uint32_t tname)
{
    return "test.t" + std::to_string(tname);
}

String PostgreSQLIntegration::truncateStatement()
{
    return "TRUNCATE";
}

bool PostgreSQLIntegration::performQuery(const String & query)
{
    if (!postgres_connection)
    {
        LOG_ERROR(fc.log, "Not connected to PostgreSQL");
        return false;
    }
    try
    {
        pqxx::work w(*(postgres_connection.get()));

        out_file << query << std::endl;
        /// Ignore the query result set
        const auto u = w.exec(query);
        UNUSED(u);
        w.commit();
        return true;
    }
    catch (std::exception const & e)
    {
        LOG_ERROR(fc.log, "PostgreSQL query: {} Error: {}", query, e.what());
        return false;
    }
}

String PostgreSQLIntegration::columnTypeAsString(RandomGenerator & rg, const bool is_deterministic, SQLType * tp) const
{
    if (!is_deterministic && rg.nextSmallNumber() < 4)
    {
        /// Use a random PostgreSQL type
        String baseType;
        const uint32_t nopt = rg.nextMediumNumber();

        if (nopt < 81)
        {
            static const std::vector<String> & baseTypes
                = {"SMALLINT",  "INTEGER",   "BIGINT",   "NUMERIC", "DECIMAL", "REAL",    "DOUBLE PRECISION", "SMALLSERIAL", "SERIAL",
                   "BIGSERIAL", "MONEY",     "TEXT",     "BPCHAR",  "BYTEA",   "TIME",    "TIMESTAMP",        "DATE",        "BOOLEAN",
                   "POINT",     "LINE",      "LSEG",     "BOX",     "PATH",    "POLYGON", "CIRCLE",           "CIDR",        "INET",
                   "MACADDR",   "MACADDR8",  "UUID",     "XML",     "JSON",    "JSONB",   "int4range",        "int8range",   "numrange",
                   "tsrange",   "tstzrange", "daterange"};
            baseType = rg.pickRandomly(baseTypes);
        }
        else if (nopt < 86)
        {
            /// Character types
            std::uniform_int_distribution<uint32_t> lengths(1, 255);
            static const std::vector<String> & prefixes = {"", "VAR", "BP"};

            baseType = fmt::format("{}CHAR({})", rg.pickRandomly(prefixes), lengths(rg.generator));
        }
        else if (nopt < 91)
        {
            /// Numeric/Decimal
            std::uniform_int_distribution<uint32_t> precisions(0, 38);
            const uint32_t precision = precisions(rg.generator);
            std::uniform_int_distribution<uint32_t> scales(0, precision);

            baseType = fmt::format("{}({},{})", rg.nextBool() ? "NUMERIC" : "DECIMAL", precision, scales(rg.generator));
        }
        else if (nopt < 96)
        {
            /// Bit types
            std::uniform_int_distribution<uint32_t> lengths(1, 64);

            baseType = fmt::format("BIT{}({})", rg.nextBool() ? " VARYING" : "", lengths(rg.generator));
        }
        else
        {
            /// Time(stamp) with timezone
            std::uniform_int_distribution<uint32_t> lengths(0, 6);

            baseType
                = fmt::format("TIME{}({}){}", rg.nextBool() ? "STAMP" : "", lengths(rg.generator), rg.nextBool() ? " WITH TIME ZONE" : "");
        }

        if (rg.nextSmallNumber() < 3)
        {
            /// Generate array type
            const uint32_t ndimensions = rg.nextMediumNumber() < 81 ? 1 : (rg.nextMediumNumber() % 4) + 1;

            for (uint32_t i = 0; i < ndimensions; i++)
            {
                baseType += "[]";
            }
        }
        return baseType;
    }
    return tp->PostgreSQLtypeName(rg, false);
}

#else
std::unique_ptr<PostgreSQLIntegration>
PostgreSQLIntegration::testAndAddPostgreSQLIntegration(FuzzConfig & fcc, const ServerCredentials &, const bool)
{
    LOG_INFO(fcc.log, "ClickHouse not compiled with PostgreSQL connector, skipping PostgreSQL integration");
    return nullptr;
}
#endif

#if defined USE_SQLITE && USE_SQLITE
void SQLiteIntegration::closeSQLiteConnection(sqlite3 * sqlite)
{
    if (sqlite)
    {
        sqlite3_close(sqlite);
    }
}

std::unique_ptr<SQLiteIntegration> SQLiteIntegration::testAndAddSQLiteIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
{
    sqlite3 * scon = nullptr;
    const std::filesystem::path client_spath = fcc.client_file_path / "sqlite.db";
    const std::filesystem::path server_spath = fcc.server_file_path / "sqlite.db";

    if (sqlite3_open(client_spath.c_str(), &scon) != SQLITE_OK)
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
        return std::make_unique<SQLiteIntegration>(fcc, scc, SQLiteUniqueKeyPtr(scon, closeSQLiteConnection), server_spath);
    }
}

void SQLiteIntegration::setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te)
{
    te->add_params()->set_svalue(sqlite_path.generic_string());
    te->add_params()->set_svalue(tname);
}

String SQLiteIntegration::getTableName(std::shared_ptr<SQLDatabase>, const uint32_t tname)
{
    return "t" + std::to_string(tname);
}

String SQLiteIntegration::truncateStatement()
{
    return "DELETE FROM";
}

bool SQLiteIntegration::performQuery(const String & query)
{
    char * err_msg = nullptr;

    if (!sqlite_connection)
    {
        LOG_ERROR(fc.log, "Not connected to SQLite");
        return false;
    }
    out_file << query << std::endl;
    if (sqlite3_exec(sqlite_connection.get(), query.c_str(), nullptr, nullptr, &err_msg) != SQLITE_OK)
    {
        LOG_ERROR(fc.log, "SQLite query: {} Error: {}", query, err_msg);
        sqlite3_free(err_msg);
        return false;
    }
    return true;
}

String SQLiteIntegration::columnTypeAsString(RandomGenerator & rg, const bool is_deterministic, SQLType * tp) const
{
    if (!is_deterministic && rg.nextSmallNumber() < 4)
    {
        /// Use a random SQLite type
        const uint32_t nopt = rg.nextMediumNumber();

        if (nopt < 91)
        {
            static const std::vector<String> & baseTypes
                = {"TEXT",    "CLOB", "STRING",   "NUMERIC", "DECIMAL",          "MONEY",  "BOOLEAN",
                   "TIME",    "DATE", "DATETIME", "INT",     "INTEGER",          "BIGINT", "SMALLINT",
                   "TINYINT", "REAL", "DOUBLE",   "FLOAT",   "DOUBLE PRECISION", "BLOB",   "BINARY",
                   "BYTEA"};
            return rg.pickRandomly(baseTypes);
        }
        else if (nopt < 96)
        {
            /// Decimal
            std::uniform_int_distribution<uint32_t> precisions(0, 20);
            const uint32_t precision = precisions(rg.generator);
            std::uniform_int_distribution<uint32_t> scales(UINT32_C(0), precision);

            return fmt::format("DECIMAL({},{})", precision, scales(rg.generator));
        }
        else
        {
            /// Character types
            std::uniform_int_distribution<uint32_t> lengths(1, 65535);
            static const std::vector<String> & baseTypes = {"CHARACTER", "VARCHAR", "NCHAR"};

            return fmt::format("{}({})", rg.pickRandomly(baseTypes), lengths(rg.generator));
        }
    }
    return tp->SQLitetypeName(rg, false);
}
#else
std::unique_ptr<SQLiteIntegration> SQLiteIntegration::testAndAddSQLiteIntegration(FuzzConfig & fcc, const ServerCredentials &)
{
    LOG_INFO(fcc.log, "ClickHouse not compiled with SQLite connector, skipping SQLite integration");
    return nullptr;
}
#endif

void RedisIntegration::setEngineDetails(RandomGenerator & rg, const SQLBase &, const String &, TableEngine * te)
{
    te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
    te->add_params()->set_num(rg.nextBool() ? 0 : rg.nextLargeNumber() % 16);
    te->add_params()->set_svalue(sc.password);
    te->add_params()->set_num(rg.nextBool() ? 16 : rg.nextLargeNumber() % 33);
}

bool RedisIntegration::performIntegration(
    RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t, const bool, const bool, std::vector<ColumnPathChain> &)
{
    return true;
}

#if defined USE_MONGODB && USE_MONGODB
std::unique_ptr<MongoDBIntegration> MongoDBIntegration::testAndAddMongoDBIntegration(FuzzConfig & fcc, const ServerCredentials & scc)
{
    String connection_str = "mongodb://";

    if (!scc.user.empty())
    {
        connection_str += fmt::format("{}{}@", scc.user, scc.password.empty() ? "" : (":" + scc.password));
    }
    connection_str += fmt::format("{}={}", scc.hostname, scc.port);

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
        return std::make_unique<MongoDBIntegration>(fcc, scc, client, db);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(fcc.log, "MongoDB connection error: {}", e.what());
        return nullptr;
    }
}

void MongoDBIntegration::setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te)
{
    te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
    te->add_params()->set_svalue(sc.database);
    te->add_params()->set_svalue(tname);
    te->add_params()->set_svalue(sc.user);
    te->add_params()->set_svalue(sc.password);
}

template <typename T>
constexpr bool is_document = std::is_same_v<T, bsoncxx::v_noabi::builder::stream::document>;

template <typename T>
void MongoDBIntegration::documentAppendBottomType(RandomGenerator & rg, const String & cname, T & output, SQLType * tp)
{
    IntType * itp;
    DateType * dtp;
    DateTimeType * dttp;
    DecimalType * detp;
    StringType * stp;
    EnumType * etp;
    GeoType * gtp;

    if ((itp = dynamic_cast<IntType *>(tp)))
    {
        switch (itp->size)
        {
            case 8:
            case 16:
            case 32: {
                const int32_t val = rg.nextRandomInt32();

                if constexpr (is_document<T>)
                {
                    output << cname << val;
                }
                else
                {
                    output << val;
                }
            }
            break;
            case 64: {
                const int64_t val = rg.nextRandomInt64();

                if constexpr (is_document<T>)
                {
                    output << cname << val;
                }
                else
                {
                    output << val;
                }
            }
            break;
            default: {
                HugeInt val(rg.nextRandomInt64(), rg.nextRandomUInt64());

                if constexpr (is_document<T>)
                {
                    output << cname << val.toString();
                }
                else
                {
                    output << val.toString();
                }
            }
        }
    }
    else if (dynamic_cast<FloatType *>(tp))
    {
        String buf;
        const uint32_t next_option = rg.nextLargeNumber();

        if (next_option < 25)
        {
            buf = "nan";
        }
        else if (next_option < 49)
        {
            buf = "inf";
        }
        else if (next_option < 73)
        {
            buf = "0.0";
        }
        else
        {
            std::uniform_int_distribution<uint32_t> next_dist(0, 8);
            const uint32_t left = next_dist(rg.generator);
            const uint32_t right = next_dist(rg.generator);

            buf = appendDecimal(rg, left, right);
        }
        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((dtp = dynamic_cast<DateType *>(tp)))
    {
        const bsoncxx::types::b_date val(
            {std::chrono::milliseconds(rg.nextBool() ? static_cast<uint64_t>(rg.nextRandomUInt32()) : rg.nextRandomUInt64())});

        if constexpr (is_document<T>)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((dttp = dynamic_cast<DateTimeType *>(tp)))
    {
        String buf = dttp->extended ? rg.nextDateTime64(rg.nextBool()) : rg.nextDateTime(rg.nextBool());

        if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((detp = dynamic_cast<DecimalType *>(tp)))
    {
        const uint32_t right = detp->scale.value_or(0);
        const uint32_t left = detp->precision.value_or(10) - right;
        String buf = appendDecimal(rg, left, right);

        if (rg.nextBool())
        {
            bsoncxx::types::b_decimal128 decimal_value(buf.c_str());

            if constexpr (is_document<T>)
            {
                output << cname << decimal_value;
            }
            else
            {
                output << decimal_value;
            }
        }
        else if constexpr (is_document<T>)
        {
            output << cname << buf;
        }
        else
        {
            output << buf;
        }
    }
    else if ((stp = dynamic_cast<StringType *>(tp)))
    {
        const uint32_t limit = stp->precision.value_or(rg.nextRandomUInt32() % 1009);

        if (rg.nextBool())
        {
            for (uint32_t i = 0; i < limit; i++)
            {
                binary_data.emplace_back(static_cast<char>(rg.nextRandomInt8()));
            }
            bsoncxx::types::b_binary val{
                bsoncxx::binary_sub_type::k_binary, limit, reinterpret_cast<const std::uint8_t *>(binary_data.data())};
            if constexpr (is_document<T>)
            {
                output << cname << val;
            }
            else
            {
                output << val;
            }
            binary_data.clear();
        }
        else
        {
            if constexpr (is_document<T>)
            {
                output << cname << rg.nextString("", true, limit);
            }
            else
            {
                output << rg.nextString("", true, limit);
            }
        }
    }
    else if (dynamic_cast<const BoolType *>(tp))
    {
        const bool val = rg.nextBool();

        if constexpr (is_document<T>)
        {
            output << cname << val;
        }
        else
        {
            output << val;
        }
    }
    else if ((etp = dynamic_cast<EnumType *>(tp)))
    {
        const EnumValue & nvalue = rg.pickRandomly(etp->values);

        if constexpr (is_document<T>)
        {
            output << cname << nvalue.val;
        }
        else
        {
            output << nvalue.val;
        }
    }
    else if (dynamic_cast<UUIDType *>(tp))
    {
        if constexpr (is_document<T>)
        {
            output << cname << rg.nextUUID();
        }
        else
        {
            output << rg.nextUUID();
        }
    }
    else if (dynamic_cast<IPv4Type *>(tp))
    {
        if constexpr (is_document<T>)
        {
            output << cname << rg.nextIPv4();
        }
        else
        {
            output << rg.nextIPv4();
        }
    }
    else if (dynamic_cast<IPv6Type *>(tp))
    {
        if constexpr (is_document<T>)
        {
            output << cname << rg.nextIPv6();
        }
        else
        {
            output << rg.nextIPv6();
        }
    }
    else if (dynamic_cast<JSONType *>(tp))
    {
        std::uniform_int_distribution<int> dopt(1, 10);
        std::uniform_int_distribution<int> wopt(1, 10);

        if constexpr (is_document<T>)
        {
            output << cname << strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator));
        }
        else
        {
            output << strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator));
        }
    }
    else if ((gtp = dynamic_cast<GeoType *>(tp)))
    {
        if constexpr (is_document<T>)
        {
            output << cname << strAppendGeoValue(rg, gtp->geotype);
        }
        else
        {
            output << strAppendGeoValue(rg, gtp->geotype);
        }
    }
    else
    {
        chassert(0);
    }
}

void MongoDBIntegration::documentAppendArray(
    RandomGenerator & rg, const String & cname, bsoncxx::builder::stream::document & document, ArrayType * at)
{
    std::uniform_int_distribution<uint64_t> nested_rows_dist(fc.min_nested_rows, fc.max_nested_rows);
    const uint64_t limit = nested_rows_dist(rg.generator);
    /// Array
    auto array = document << cname << bsoncxx::builder::stream::open_array;
    SQLType * tp = at->subtype;
    Nullable * nl;
    VariantType * vtp;
    LowCardinality * lc;

    for (uint64_t i = 0; i < limit; i++)
    {
        const uint32_t nopt = rg.nextLargeNumber();

        if (nopt < 31)
        {
            /// Null Value
            array << bsoncxx::types::b_null{};
        }
        else if (nopt < 41)
        {
            /// Oid Value
            array << bsoncxx::oid{};
        }
        else if (nopt < 46)
        {
            /// Max-Key Value
            array << bsoncxx::types::b_maxkey{};
        }
        else if (nopt < 51)
        {
            /// Min-Key Value
            array << bsoncxx::types::b_minkey{};
        }
        else if (
            dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
            || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
            || dynamic_cast<EnumType *>(tp) || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp)
            || dynamic_cast<JSONType *>(tp) || dynamic_cast<GeoType *>(tp))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, at->subtype);
        }
        else if ((lc = dynamic_cast<LowCardinality *>(tp)))
        {
            if ((nl = dynamic_cast<Nullable *>(lc->subtype)))
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
            }
            else
            {
                documentAppendBottomType<decltype(array)>(rg, "", array, lc->subtype);
            }
        }
        else if ((nl = dynamic_cast<Nullable *>(tp)))
        {
            documentAppendBottomType<decltype(array)>(rg, "", array, nl->subtype);
        }
        else if (dynamic_cast<ArrayType *>(tp))
        {
            array << bsoncxx::builder::stream::open_array << 1 << bsoncxx::builder::stream::close_array;
        }
        else if ((vtp = dynamic_cast<VariantType *>(tp)))
        {
            if (vtp->subtypes.empty())
            {
                /// Null Value
                array << bsoncxx::types::b_null{};
            }
            else
            {
                array << 1;
            }
        }
    }
    array << bsoncxx::builder::stream::close_array;
}

void MongoDBIntegration::documentAppendAnyValue(
    RandomGenerator & rg, const String & cname, bsoncxx::builder::stream::document & document, SQLType * tp)
{
    Nullable * nl;
    ArrayType * at;
    VariantType * vtp;
    LowCardinality * lc;
    const uint32_t nopt = rg.nextLargeNumber();

    if (nopt < 31)
    {
        /// Null Value
        document << cname << bsoncxx::types::b_null{};
    }
    else if (nopt < 41)
    {
        /// Oid Value
        document << cname << bsoncxx::oid{};
    }
    else if (nopt < 46)
    {
        /// Max-Key Value
        document << cname << bsoncxx::types::b_maxkey{};
    }
    else if (nopt < 51)
    {
        /// Min-Key Value
        document << cname << bsoncxx::types::b_minkey{};
    }
    else if (
        dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
        || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
        || dynamic_cast<EnumType *>(tp) || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp)
        || dynamic_cast<JSONType *>(tp) || dynamic_cast<GeoType *>(tp))
    {
        documentAppendBottomType<bsoncxx::v_noabi::builder::stream::document>(rg, cname, document, tp);
    }
    else if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, lc->subtype);
    }
    else if ((nl = dynamic_cast<Nullable *>(tp)))
    {
        documentAppendAnyValue(rg, cname, document, nl->subtype);
    }
    else if ((at = dynamic_cast<ArrayType *>(tp)))
    {
        documentAppendArray(rg, cname, document, at);
    }
    else if ((vtp = dynamic_cast<VariantType *>(tp)))
    {
        if (vtp->subtypes.empty())
        {
            /// Null Value
            document << cname << bsoncxx::types::b_null{};
        }
        else
        {
            documentAppendAnyValue(rg, cname, document, rg.pickRandomly(vtp->subtypes));
        }
    }
    else
    {
        chassert(0);
    }
}

bool MongoDBIntegration::performIntegration(
    RandomGenerator & rg,
    std::shared_ptr<SQLDatabase>,
    const uint32_t tname,
    const bool can_shuffle,
    const bool,
    std::vector<ColumnPathChain> & entries)
{
    try
    {
        const bool permute = can_shuffle && rg.nextBool();
        const bool miss_cols = rg.nextBool();
        const uint32_t ndocuments = rg.nextMediumNumber();
        const String str_tname = "t" + std::to_string(tname);
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
                    /// Sometimes the column is missing
                    documentAppendAnyValue(rg, entry.getBottomName(), document, entry.getBottomType());
                    chassert(entry.path.size() == 1);
                }
            }
            documents.emplace_back(document << bsoncxx::builder::stream::finalize);
        }
        /// Collection name
        out_file << str_tname << std::endl;
        for (const auto & doc : documents)
        {
            /// Write each JSON document on a new line
            out_file << bsoncxx::to_json(doc.view()) << std::endl;
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
#else
std::unique_ptr<MongoDBIntegration> MongoDBIntegration::testAndAddMongoDBIntegration(FuzzConfig & fcc, const ServerCredentials &)
{
    LOG_INFO(fcc.log, "ClickHouse not compiled with MongoDB connector, skipping MongoDB integration");
    return nullptr;
}
#endif

bool MinIOIntegration::sendRequest(const String & resource)
{
    struct tm ttm;
    ssize_t nbytes = 0;
    int sock = -1;
    int error = 0;
    char buffer[1024];
    char found_ip[1024];
    const std::time_t time = std::time({});
    DB::WriteBufferFromOwnString sign_cmd;
    DB::WriteBufferFromOwnString sign_out;
    DB::WriteBufferFromOwnString sign_err;
    DB::WriteBufferFromOwnString http_request;
    struct addrinfo hints = {};
    struct addrinfo * result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    if (!std::sprintf(buffer, "%" PRIu32 "", sc.port))
    {
        LOG_ERROR(fc.log, "Buffer size was to small to fit result");
        return false;
    }
    if ((error = getaddrinfo(sc.hostname.c_str(), buffer, &hints, &result)) != 0)
    {
        if (error == EAI_SYSTEM)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            LOG_ERROR(fc.log, "getaddrinfo error: {}", buffer);
        }
        else
        {
            LOG_ERROR(fc.log, "getaddrinfo error: {}", gai_strerror(error));
        }
        return false;
    }

    /// Loop through results
    for (const struct addrinfo * p = result; p; p = p->ai_next)
    {
        if ((sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            LOG_ERROR(fc.log, "Could not connect: {}", buffer);
            return false;
        }
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
        {
            if ((error = getnameinfo(p->ai_addr, p->ai_addrlen, found_ip, sizeof(found_ip), nullptr, 0, NI_NUMERICHOST)) != 0)
            {
                if (error == EAI_SYSTEM)
                {
                    strerror_r(errno, buffer, sizeof(buffer));
                    LOG_ERROR(fc.log, "getnameinfo error: {}", buffer);
                }
                else
                {
                    LOG_ERROR(fc.log, "getnameinfo error: {}", gai_strerror(error));
                }
                close(sock);
                return false;
            }
            break;
        }
        close(sock);
        sock = -1;
    }
    if (sock == -1)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        LOG_ERROR(fc.log, "Could not connect: {}", buffer);
        freeaddrinfo(result);
        return false;
    }
    freeaddrinfo(result);
    SCOPE_EXIT({
        if (sock > -1)
        {
            close(sock);
        }
    });
    if (!gmtime_r(&time, &ttm))
    {
        strerror_r(errno, buffer, sizeof(buffer));
        LOG_ERROR(fc.log, "Could not convert time: {}", buffer);
        return false;
    }
    if (!std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S %z", &ttm))
    {
        LOG_ERROR(fc.log, "Buffer size was to small to fit result");
        return false;
    }
    sign_cmd << R"(printf "PUT\n\napplication/octet-stream\n)" << buffer << "\\n"
             << resource << "\""
             << " | openssl sha1 -hmac " << sc.password << " -binary | base64";
    auto res = DB::ShellCommand::execute(sign_cmd.str());
    res->in.close();
    copyData(res->out, sign_out);
    copyData(res->err, sign_err);
    res->wait();
    if (!sign_err.str().empty())
    {
        LOG_ERROR(fc.log, "Error while executing shell command: {}", sign_err.str());
        return false;
    }

    http_request << "PUT " << resource << " HTTP/1.1\n"
                 << "Host: " << found_ip << ":" << std::to_string(sc.port) << "\n"
                 << "Accept: */*\n"
                 << "Date: " << buffer << "\n"
                 << "Content-Type: application/octet-stream\n"
                 << "Authorization: AWS " << sc.user << ":" << sign_out.str() << "Content-Length: 0\n\n\n";

    if (send(sock, http_request.str().c_str(), http_request.str().length(), 0) != static_cast<int>(http_request.str().length()))
    {
        strerror_r(errno, buffer, sizeof(buffer));
        LOG_ERROR(fc.log, "Error sending request \"{}\": {}", http_request.str(), buffer);
        return false;
    }
    if ((nbytes = read(sock, buffer, sizeof(buffer))) < 0)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        LOG_ERROR(fc.log, "Error reading request \"{}\" result: {}", http_request.str(), buffer);
        return false;
    }
    if (nbytes < 13 || std::memcmp(buffer + 9, "200", 3) != 0)
    {
        LOG_ERROR(fc.log, "Request \"{}\" was not successful", http_request.str());
        return false;
    }
    return true;
}

String MinIOIntegration::getConnectionURL()
{
    return "http://" + sc.hostname + ":" + std::to_string(sc.port) + sc.database + "/";
}

void MinIOIntegration::setEngineDetails(RandomGenerator &, const SQLBase & b, const String & tname, TableEngine * te)
{
    te->add_params()->set_svalue(getConnectionURL() + "file" + tname.substr(1) + (b.isS3QueueEngine() ? "/*" : ""));
    te->add_params()->set_svalue(sc.user);
    te->add_params()->set_svalue(sc.password);
}

void MinIOIntegration::setBackupDetails(const String & filename, BackupRestore * br)
{
    br->add_out_params(getConnectionURL() + filename);
    br->add_out_params(sc.user);
    br->add_out_params(sc.password);
}

bool MinIOIntegration::performIntegration(
    RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t tname, const bool, const bool, std::vector<ColumnPathChain> &)
{
    return sendRequest(sc.database + "/file" + std::to_string(tname));
}

void AzuriteIntegration::setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te)
{
    te->add_params()->set_svalue(sc.hostname);
    te->add_params()->set_svalue(sc.container);
    te->add_params()->set_svalue("file" + tname.substr(1));
    te->add_params()->set_svalue(sc.user);
    te->add_params()->set_svalue(sc.password);
}

void AzuriteIntegration::setBackupDetails(const String & filename, BackupRestore * br)
{
    br->add_out_params(sc.hostname);
    br->add_out_params(sc.container);
    br->add_out_params(filename);
    br->add_out_params(sc.user);
    br->add_out_params(sc.password);
}

bool AzuriteIntegration::performIntegration(
    RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t, const bool, const bool, std::vector<ColumnPathChain> &)
{
    return true;
}

String HTTPIntegration::getConnectionURL()
{
    return "http://" + sc.hostname + ":" + std::to_string(sc.port) + "/";
}

void HTTPIntegration::setEngineDetails(RandomGenerator &, const SQLBase &, const String & tname, TableEngine * te)
{
    te->add_params()->set_svalue(getConnectionURL() + "file" + tname.substr(1));
}

bool HTTPIntegration::performIntegration(
    RandomGenerator &, std::shared_ptr<SQLDatabase>, const uint32_t, const bool, const bool, std::vector<ColumnPathChain> &)
{
    return true;
}

ExternalIntegrations::ExternalIntegrations(FuzzConfig & fcc)
    : fc(fcc)
{
    if (fc.mysql_server.has_value())
    {
        mysql = MySQLIntegration::testAndAddMySQLConnection(fc, fc.mysql_server.value(), fc.read_log, "MySQL");
    }
    if (fc.postgresql_server.has_value())
    {
        postresql = PostgreSQLIntegration::testAndAddPostgreSQLIntegration(fc, fc.postgresql_server.value(), fc.read_log);
    }
    if (fc.sqlite_server.has_value())
    {
        sqlite = SQLiteIntegration::testAndAddSQLiteIntegration(fc, fc.sqlite_server.value());
    }
    if (fc.mongodb_server.has_value())
    {
        mongodb = MongoDBIntegration::testAndAddMongoDBIntegration(fc, fc.mongodb_server.value());
    }
    if (fc.redis_server.has_value())
    {
        redis = std::make_unique<RedisIntegration>(fc, fc.redis_server.value());
    }
    if (fc.minio_server.has_value())
    {
        minio = std::make_unique<MinIOIntegration>(fc, fc.minio_server.value());
    }
    if (fc.azurite_server.has_value())
    {
        azurite = std::make_unique<AzuriteIntegration>(fc, fc.azurite_server.value());
    }
    if (fc.http_server.has_value())
    {
        http = std::make_unique<HTTPIntegration>(fc, fc.http_server.value());
    }
    if (fc.clickhouse_server.has_value())
    {
        clickhouse = MySQLIntegration::testAndAddMySQLConnection(fc, fc.clickhouse_server.value(), fc.read_log, "ClickHouse");
    }
}

void ExternalIntegrations::createExternalDatabaseTable(
    RandomGenerator & rg, const IntegrationCall dc, const SQLBase & b, std::vector<ColumnPathChain> & entries, TableEngine * te)
{
    const String & tname = "t" + std::to_string(b.tname);

    requires_external_call_check++;
    switch (dc)
    {
        case IntegrationCall::MySQL:
            next_calls_succeeded.emplace_back(mysql->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            mysql->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::PostgreSQL:
            next_calls_succeeded.emplace_back(postresql->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            postresql->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::SQLite:
            next_calls_succeeded.emplace_back(sqlite->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            sqlite->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::MongoDB:
            next_calls_succeeded.emplace_back(mongodb->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            mongodb->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::Redis:
            next_calls_succeeded.emplace_back(redis->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            redis->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::MinIO:
            next_calls_succeeded.emplace_back(minio->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            minio->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::Azurite:
            next_calls_succeeded.emplace_back(azurite->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            azurite->setEngineDetails(rg, b, tname, te);
            break;
        case IntegrationCall::HTTP:
            next_calls_succeeded.emplace_back(http->performIntegration(rg, b.db, b.tname, true, b.is_deterministic, entries));
            http->setEngineDetails(rg, b, tname, te);
            break;
    }
}

void ExternalIntegrations::createPeerTable(
    RandomGenerator & rg, const PeerTableDatabase pt, const SQLTable & t, const CreateTable * ct, std::vector<ColumnPathChain> & entries)
{
    requires_external_call_check++;
    switch (pt)
    {
        case PeerTableDatabase::ClickHouse:
            next_calls_succeeded.emplace_back(clickhouse->performCreatePeerTable(rg, true, t, ct, entries));
            break;
        case PeerTableDatabase::MySQL:
            next_calls_succeeded.emplace_back(mysql->performCreatePeerTable(rg, false, t, ct, entries));
            break;
        case PeerTableDatabase::PostgreSQL:
            next_calls_succeeded.emplace_back(postresql->performCreatePeerTable(rg, false, t, ct, entries));
            break;
        case PeerTableDatabase::SQLite:
            next_calls_succeeded.emplace_back(sqlite->performCreatePeerTable(rg, false, t, ct, entries));
            break;
        case PeerTableDatabase::None:
            chassert(0);
            break;
    }
}

bool ExternalIntegrations::truncatePeerTableOnRemote(const SQLTable & t)
{
    switch (t.peer_table)
    {
        case PeerTableDatabase::ClickHouse:
            return clickhouse->truncatePeerTableOnRemote(t);
        case PeerTableDatabase::MySQL:
            return mysql->truncatePeerTableOnRemote(t);
        case PeerTableDatabase::PostgreSQL:
            return postresql->truncatePeerTableOnRemote(t);
        case PeerTableDatabase::SQLite:
            return sqlite->truncatePeerTableOnRemote(t);
        case PeerTableDatabase::None:
            chassert(0);
            return false;
    }
}

bool ExternalIntegrations::optimizeTableForOracle(const PeerTableDatabase pt, const SQLTable & t)
{
    switch (t.peer_table)
    {
        case PeerTableDatabase::ClickHouse:
            return clickhouse->optimizeTableForOracle(pt, t);
        case PeerTableDatabase::MySQL:
        case PeerTableDatabase::PostgreSQL:
        case PeerTableDatabase::SQLite:
        case PeerTableDatabase::None:
            return false;
    }
}

void ExternalIntegrations::dropPeerTableOnRemote(const SQLTable & t)
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

void ExternalIntegrations::setBackupDetails(const IntegrationCall dc, const String & filename, BackupRestore * br)
{
    switch (dc)
    {
        case IntegrationCall::MinIO:
            minio->setBackupDetails(filename, br);
            break;
        case IntegrationCall::Azurite:
            azurite->setBackupDetails(filename, br);
            break;
        default:
            chassert(0);
            break;
    }
}

bool ExternalIntegrations::performQuery(const PeerTableDatabase pt, const String & query)
{
    switch (pt)
    {
        case PeerTableDatabase::ClickHouse:
            return clickhouse->performQuery(query);
        case PeerTableDatabase::MySQL:
            return mysql->performQuery(query);
        case PeerTableDatabase::PostgreSQL:
            return postresql->performQuery(query);
        case PeerTableDatabase::SQLite:
            return sqlite->performQuery(query);
        case PeerTableDatabase::None:
            return false;
    }
}

std::filesystem::path ExternalIntegrations::getDatabaseDataDir(const PeerTableDatabase pt, const bool server) const
{
    switch (pt)
    {
        case PeerTableDatabase::ClickHouse:
            return clickhouse->sc.user_files_dir / "fuzz.data";
        case PeerTableDatabase::MySQL:
            return mysql->sc.user_files_dir / "fuzz.data";
        case PeerTableDatabase::PostgreSQL:
            return postresql->sc.user_files_dir / "fuzz.data";
        case PeerTableDatabase::SQLite:
            return sqlite->sc.user_files_dir / "fuzz.data";
        case PeerTableDatabase::None:
            return server ? fc.fuzz_server_out : fc.fuzz_client_out;
    }
}

bool ExternalIntegrations::getPerformanceMetricsForLastQuery(const PeerTableDatabase pt, PerformanceResult & res)
{
    String buf;
    std::error_code ec;
    const std::filesystem::path client_out_path = this->getDatabaseDataDir(pt, false);
    const std::filesystem::path server_out_path = this->getDatabaseDataDir(pt, true);

    res.metrics.clear();
    if (!std::filesystem::remove(client_out_path, ec) && ec)
    {
        LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
        return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(fc.flush_log_wait_time));
    if (clickhouse->performQueryOnServerOrRemote(
            pt,
            fmt::format(
                "INSERT INTO TABLE FUNCTION file('{}', 'TabSeparated', 'c0 UInt64, c1 UInt64, c2 UInt64') SELECT query_duration_ms, "
                "memory_usage, read_bytes FROM system.query_log WHERE log_comment = 'measure_performance' AND type = 'QueryFinish' ORDER "
                "BY event_time_microseconds DESC LIMIT 1;",
                server_out_path.generic_string())))
    {
        std::ifstream infile(client_out_path);
        if (std::getline(infile, buf) && buf.size() > 1)
        {
            if (buf[buf.size() - 1] == '\r')
            {
                buf.pop_back();
            }
            const auto tabchar1 = buf.find('\t');
            const auto substr = buf.substr(tabchar1 + 1);
            const auto tabchar2 = substr.find('\t');

            res.metrics.insert(
                {{"query_time", static_cast<uint64_t>(std::stoull(buf))},
                 {"query_memory", static_cast<uint64_t>(std::stoull(buf.substr(tabchar1 + 1)))},
                 {"query_bytes_read", static_cast<uint64_t>(std::stoull(substr.substr(tabchar2 + 1)))}});
            return true;
        }
    }
    return false;
}

void ExternalIntegrations::setDefaultSettings(const PeerTableDatabase pt, const DB::Strings & settings)
{
    for (const auto & entry : settings)
    {
        /// Some settings may not exist in earlier ClickHouse versions, so we can ignore the errors here
        const auto u = clickhouse->performQueryOnServerOrRemote(pt, fmt::format("SET {} = 1;", entry));
        UNUSED(u);
    }
}

void ExternalIntegrations::replicateSettings(const PeerTableDatabase pt)
{
    String buf;
    String replaced;
    std::error_code ec;

    if (!std::filesystem::remove(fc.fuzz_client_out, ec) && ec)
    {
        LOG_ERROR(fc.log, "Could not remove file: {}", ec.message());
        return;
    }
    if (fc.processServerQuery(
            false,
            fmt::format(
                "SELECT `name`, `value` FROM system.settings WHERE changed = 1 INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;",
                fc.fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fc.fuzz_client_out);
        while (std::getline(infile, buf) && buf.size() > 1)
        {
            if (buf[buf.size() - 1] == '\r')
            {
                buf.pop_back();
            }
            const auto tabchar = buf.find('\t');
            const auto nname = buf.substr(0, tabchar);
            const auto nvalue = buf.substr(tabchar + 1);

            replaced.resize(0);
            for (const auto & c : nvalue)
            {
                switch (c)
                {
                    case '\'':
                        replaced += "\\'";
                        break;
                    case '\\':
                        replaced += "\\\\";
                        break;
                    case '\b':
                        replaced += "\\b";
                        break;
                    case '\f':
                        replaced += "\\f";
                        break;
                    case '\r':
                        replaced += "\\r";
                        break;
                    case '\n':
                        replaced += "\\n";
                        break;
                    case '\t':
                        replaced += "\\t";
                        break;
                    case '\0':
                        replaced += "\\0";
                        break;
                    case '\a':
                        replaced += "\\a";
                        break;
                    case '\v':
                        replaced += "\\v";
                        break;
                    default:
                        replaced += c;
                }
            }
            /// Some settings may not exist in earlier ClickHouse versions, so we can ignore the errors here
            auto u = clickhouse->performQueryOnServerOrRemote(pt, fmt::format("SET {} = '{}';", nname, replaced));
            UNUSED(u);
            buf.resize(0);
        }
    }
}

}
