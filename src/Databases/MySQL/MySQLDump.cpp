#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MySQLDump.h>

#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/MySQL/MySQLReplication.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <Databases/IDatabase.h>
#include <Databases/MySQL/MySQLUtils.h>
#include <Formats/MySQLBlockInputStream.h>
#include <IO/Progress.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

static inline void cleanOutdatedTables(const String & database_name, const Context & context)
{
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
    const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name);

    for (auto iterator = clean_database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        Context query_context = createQueryContext(context);
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        String table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());
        tryToExecuteQuery(" DROP TABLE " + table_name, query_context, database_name, comment);
    }
}

static inline void dumpDataForTables(
    mysqlxx::Pool::Entry & connection,
    std::unordered_map<String, String>& need_dumping_tables,
    const String & query_prefix,
    const String & database_name,
    const String & mysql_database_name,
    const Context & context,
    const std::function<bool()> & is_cancelled)
{
    auto iterator = need_dumping_tables.begin();
    for (; iterator != need_dumping_tables.end() && !is_cancelled(); ++iterator)
    {
        const auto & table_name = iterator->first;
        Context query_context = createQueryContext(context);
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        tryToExecuteQuery(query_prefix + " " + iterator->second, query_context, database_name, comment); /// create table.

        auto out = std::make_shared<CountingBlockOutputStream>(getTableOutput(database_name, table_name, query_context));
        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
            out->getHeader(), DEFAULT_BLOCK_SIZE);

        Stopwatch watch;
        copyData(input, *out, is_cancelled);
        const Progress & progress = out->getProgress();
        LOG_INFO(&Poco::Logger::get("MaterializeMySQLSyncThread(" + database_name + ")"),
            "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec.",
            table_name,
            formatReadableQuantity(progress.written_rows),
            formatReadableSizeWithBinarySuffix(progress.written_bytes),
            watch.elapsedSeconds(),
            formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds())),
            formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
    }
}

void dumpTables(
    const Context & global_context,
    mysqlxx::Pool::Entry & connection,
    Poco::Logger * log,
    std::shared_ptr<const ConsumerDatabase> consumer,
    std::unordered_map<String, String> & need_dumping_tables,
    const std::function<bool()> & is_cancelled)
{
    if (need_dumping_tables.empty())
    {
        return;
    }

    Position position;
    position.update(
        consumer->materialize_metadata->binlog_position,
        consumer->materialize_metadata->binlog_file,
        consumer->materialize_metadata->executed_gtid_set);

    consumer->materialize_metadata->transaction(position, [&]()
    {
        cleanOutdatedTables(consumer->database_name, global_context);
        dumpDataForTables(
            connection,
            need_dumping_tables,
            consumer->getQueryPrefix(),
            consumer->database_name,
            consumer->mysql_database_name,
            global_context,
            is_cancelled);
    });

    const auto & position_message = [&]()
    {
        std::stringstream ss;
        position.dump(ss);
        return ss.str();
    };
    LOG_INFO(log, "MySQL dump database position: \n {}", position_message());
}

static std::vector<String> fetchTablesInDB(
    const mysqlxx::PoolWithFailover::Entry & connection,
    const std::string & mysql_database_name)
{
    Block header{{std::make_shared<DataTypeString>(), "table_name"}};
    String query = "SELECT TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_SCHEMA = " + quoteString(mysql_database_name);

    std::vector<String> tables_in_db;
    MySQLBlockInputStream input(connection, query, header, DEFAULT_BLOCK_SIZE);

    while (Block block = input.read())
    {
        tables_in_db.reserve(tables_in_db.size() + block.rows());
        for (size_t index = 0; index < block.rows(); ++index)
            tables_in_db.emplace_back((*block.getByPosition(0).column)[index].safeGet<String>());
    }

    return tables_in_db;
}

std::unordered_map<String, String> fetchTablesCreateQuery(
    const mysqlxx::PoolWithFailover::Entry & connection,
    const String & mysql_database_name)
{
    std::vector<String> fetch_tables = fetchTablesInDB(
        connection,
        mysql_database_name);
    std::unordered_map<String, String> tables_create_query;
    for (const auto & fetch_table_name : fetch_tables)
    {
        Block show_create_table_header{
            {std::make_shared<DataTypeString>(), "Table"},
            {std::make_shared<DataTypeString>(), "Create Table"},
        };

        MySQLBlockInputStream show_create_table(
            connection,
            "SHOW CREATE TABLE " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(fetch_table_name),
            show_create_table_header,
            DEFAULT_BLOCK_SIZE);

        Block create_query_block = show_create_table.read();
        if (!create_query_block || create_query_block.rows() != 1)
            throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

        tables_create_query[fetch_table_name] = create_query_block.getByName("Create Table").column->getDataAt(0).toString();
    }

    return tables_create_query;
}

}

#endif
