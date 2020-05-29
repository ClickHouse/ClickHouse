#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <cstdlib>
#    include <Columns/ColumnTuple.h>
#    include <DataStreams/AddingVersionsBlockOutputStream.h>
#    include <DataStreams/copyData.h>
#    include <DataTypes/DataTypeString.h>
#    include <Databases/DatabaseFactory.h>
#    include <Databases/MySQL/DataBuffers.h>
#    include <Databases/MySQL/MasterStatusInfo.h>
#    include <Databases/MySQL/queryConvert.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/MySQL/CreateQueryVisitor.h>
#    include <Interpreters/executeQuery.h>
#    include <Parsers/MySQL/ASTCreateQuery.h>
#    include <Parsers/parseQuery.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context, const String & database_name_, const String & metadata_path_,
    const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_, MySQLClient && client_)
    : IDatabase(database_name_), global_context(context.getGlobalContext()), metadata_path(metadata_path_)
    , database_engine_define(database_engine_define_->clone()), mysql_database_name(mysql_database_name_)
    , nested_database(std::make_shared<DatabaseOrdinary>(database_name_, metadata_path, global_context))
    , pool(std::move(pool_)), client(std::move(client_)), log(&Logger::get("DatabaseMaterializeMySQL"))
{
    /// TODO: 做简单的check, 失败即报错
}

BlockIO DatabaseMaterializeMySQL::tryToExecuteQuery(const String & query_to_execute, const String & comment)
{
    String query_prefix = "/*" + comment + " for " + backQuoteIfNeed(database_name) + "  Database */ ";

    try
    {
        Context context = global_context;
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        context.setCurrentQueryId(""); // generate random query_id
        return executeQuery(query_prefix + query_to_execute, context, true);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }

    LOG_DEBUG(log, "Executed query: " << query_to_execute);
}

String DatabaseMaterializeMySQL::getCreateQuery(const mysqlxx::Pool::Entry & connection, const String & table_name)
{
    Block show_create_table_header{
        {std::make_shared<DataTypeString>(), "Table"},
        {std::make_shared<DataTypeString>(), "Create Table"},
    };

    MySQLBlockInputStream show_create_table(
        connection, "SHOW CREATE TABLE " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
        show_create_table_header, DEFAULT_BLOCK_SIZE);

    Block create_query_block = show_create_table.read();
    if (!create_query_block || create_query_block.rows() != 1)
        throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

    return create_query_block.getByName("Create Table").column->getDataAt(0).toString();
}

BlockOutputStreamPtr DatabaseMaterializeMySQL::getTableOutput(const String & table_name, bool fill_version)
{
    Context context = global_context;
    context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    context.setCurrentQueryId(""); // generate random query_id

    StoragePtr write_storage = nested_database->tryGetTable(table_name);
    auto table_lock = write_storage->lockStructureForShare(true, context.getInitialQueryId(), context.getSettingsRef().lock_acquire_timeout);

    BlockOutputStreamPtr output = write_storage->write(ASTPtr{}, global_context);
    output->addTableLock(table_lock);

    return fill_version ? std::make_shared<AddingVersionsBlockOutputStream>(version, output) : output;
}

void DatabaseMaterializeMySQL::dumpDataForTables(mysqlxx::Pool::Entry & connection, const std::vector<String> & tables_name, const std::function<bool()> & is_cancelled)
{
    std::unordered_map<String, String> tables_create_query;
    for (size_t index = 0; index < tables_name.size() && !is_cancelled(); ++index)
        tables_create_query[tables_name[index]] = getCreateQuery(connection, tables_name[index]);

    auto iterator = tables_create_query.begin();
    for (; iterator != tables_create_query.end() && !is_cancelled(); ++iterator)
    {
        const auto & table_name = iterator->first;
        MySQLTableStruct table_struct = visitCreateQuery(iterator->second, global_context, database_name);
        String comment = String("Dumping ") + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name);
        tryToExecuteQuery(toCreateQuery(table_struct, global_context), comment);

        BlockOutputStreamPtr out = getTableOutput(table_name, true);
        MySQLBlockInputStream input(connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name), out->getHeader(), DEFAULT_BLOCK_SIZE);
        copyData(input, *out, is_cancelled);
    }
}

MasterStatusInfo DatabaseMaterializeMySQL::prepareSynchronized(std::unique_lock<std::mutex> & lock)
{
    while(!sync_quit.load(std::memory_order_seq_cst))
    {
        try
        {
            LOG_DEBUG(log, "Checking " + database_name + " database status.");
            while (!sync_quit && !DatabaseCatalog::instance().isDatabaseExist(database_name))
                sync_cond.wait_for(lock, std::chrono::seconds(1));

            LOG_DEBUG(log, database_name + " database status is OK.");

            mysqlxx::PoolWithFailover::Entry connection = pool.get();
            MasterStatusInfo master_info(connection, getMetadataPath() + "/.master_status", mysql_database_name);

            if (!master_info.need_dumping_tables.empty())
            {
                /// TODO: 删除所有表结构, 这可能需要考虑到仍然有查询在使用这个表.
                dumpDataForTables(connection, master_info.need_dumping_tables, [&]() { return sync_quit.load(std::memory_order_seq_cst); });
                master_info.finishDump();
            }

            client.connect();
            client.startBinlogDump(std::rand(), mysql_database_name, master_info.binlog_file, master_info.binlog_position);
            return master_info;
        }
        catch(...)
        {
            tryLogCurrentException(log, "Prepare MySQL Synchronized exception and retry");

            sleepForSeconds(1);
        }
    }

    throw Exception("", ErrorCodes::LOGICAL_ERROR);
}

void DatabaseMaterializeMySQL::synchronized()
{
    setThreadName("MySQLDBSync");

    try
    {
        std::unique_lock<std::mutex> lock{sync_mutex};

        MasterStatusInfo master_status = prepareSynchronized(lock);

        DataBuffers buffers(version, this, [&](const std::unordered_map<String, Block> & tables_data)
        {
            master_status.transaction(client.getPosition(), [&]()    /// At least once, There is only one possible reference: https://github.com/ClickHouse/ClickHouse/pull/8467
            {
                for (const auto & [table_name, data] : tables_data)
                {
                    if (!sync_quit.load(std::memory_order_seq_cst))
                    {
                        LOG_DEBUG(log, "Prepare to flush data.");
                        BlockOutputStreamPtr output = getTableOutput(table_name, false);
                        output->writePrefix();
                        output->write(data);
                        output->writeSuffix();
                        output->flush();
                        LOG_DEBUG(log, "Finish data flush.");
                    }
                }
            });
        });

        while (!sync_quit.load(std::memory_order_seq_cst))
        {
            const auto & event = client.readOneBinlogEvent();

            if (event->type() == MYSQL_WRITE_ROWS_EVENT)
            {
                WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*event);
                write_rows_event.dump();
                buffers.writeData(write_rows_event.table, write_rows_event.rows);
            }
            else if (event->type() == MYSQL_UPDATE_ROWS_EVENT)
            {
                UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*event);
                update_rows_event.dump();
                buffers.updateData(update_rows_event.table, update_rows_event.rows);
            }
            else if (event->type() == MYSQL_DELETE_ROWS_EVENT)
            {
                DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*event);
                delete_rows_event.dump();
                buffers.deleteData(delete_rows_event.table, delete_rows_event.rows);
            }
            else if (event->type() == MYSQL_QUERY_EVENT)
            {
                /// TODO: 识别, 查看是否支持的DDL, 支持的话立即刷新当前的数据, 然后执行DDL.
                buffers.flush();
            }
        }
    }
    catch(...)
    {
        tryLogCurrentException(log);
    }
}

}

#endif
