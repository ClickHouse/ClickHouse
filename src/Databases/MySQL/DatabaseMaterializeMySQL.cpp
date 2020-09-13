#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>

#    include <vector>
#    include <unordered_map>

#    include <Common/Exception.h>
#    include <Common/Stopwatch.h>
#    include <Common/quoteString.h>
#    include <Common/formatReadable.h>
#    include <Core/Block.h>
#    include <Core/Defines.h>
#    include <Core/MySQL/MySQLReplication.h>
#    include <Interpreters/Context.h>
#    include <Databases/DatabaseOrdinary.h>
#    include <Databases/MySQL/DatabaseMaterializeTablesIterator.h>
#    include <Databases/MySQL/MaterializeMySQLSyncThread.h>
#    include <Databases/MySQL/MySQLUtils.h>
#    include <DataStreams/copyData.h>
#    include <DataStreams/CountingBlockOutputStream.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/StorageMaterializeMySQL.h>
#    include <Poco/File.h>
#    include <Poco/Logger.h>
#    include <Common/setThreadName.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/Progress.h>
#    include <Interpreters/DatabaseCatalog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DatabaseMaterializeMySQL::DatabaseMaterializeMySQL(
    const Context & context,
    const String & database_name_,
    const String & metadata_path_,
    const IAST * database_engine_define_,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    std::unique_ptr<MaterializeMySQLSettings> settings_)
    : IDatabase(database_name_), global_context(context.getGlobalContext())
    , engine_define(database_engine_define_->clone())
    , nested_database(std::make_shared<DatabaseOrdinary>(database_name_, metadata_path_, context))
    , mysql_database_name(mysql_database_name_)
    , settings(std::move(settings_))
    , pool(std::move(pool_))
    , client(std::move(client_))
    , log(&Poco::Logger::get("DatabaseMaterializeMySQL"))
{
    query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(database_name) + ", " + backQuoteIfNeed(mysql_database_name) + ") ";
}

void DatabaseMaterializeMySQL::rethrowExceptionIfNeed() const
{
    std::unique_lock<std::mutex> lock(mutex);

    if (!settings->allows_query_when_mysql_lost && exception)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (Exception & ex)
        {
            throw Exception(ex);
        }
    }
}

void DatabaseMaterializeMySQL::setException(const std::exception_ptr & exception_)
{
    std::unique_lock<std::mutex> lock(mutex);
    exception = exception_;
}

ASTPtr DatabaseMaterializeMySQL::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->database = database_name;
    create_query->set(create_query->storage, engine_define);
    return create_query;
}

std::vector<String> DatabaseMaterializeMySQL::fetchTablesInDB(const mysqlxx::PoolWithFailover::Entry & connection)
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

std::unordered_map<String, String> DatabaseMaterializeMySQL::fetchTablesCreateQuery(const mysqlxx::PoolWithFailover::Entry & connection)
{
    auto fetch_tables = fetchTablesInDB(connection);
    std::unordered_map<String, String> tables_create_query;
    for (const auto & fetch_table_name : fetch_tables)
    {
        Block show_create_table_header{
            {std::make_shared<DataTypeString>(), "Table"},
            {std::make_shared<DataTypeString>(), "Create Table"},
        };

        MySQLBlockInputStream show_create_table(
            connection, "SHOW CREATE TABLE " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(fetch_table_name),
            show_create_table_header, DEFAULT_BLOCK_SIZE);

        Block create_query_block = show_create_table.read();
        if (!create_query_block || create_query_block.rows() != 1)
            throw Exception("LOGICAL ERROR mysql show create return more rows.", ErrorCodes::LOGICAL_ERROR);

        tables_create_query[fetch_table_name] = create_query_block.getByName("Create Table").column->getDataAt(0).toString();
    }

    return tables_create_query;
}

void DatabaseMaterializeMySQL::executeDumpQueries(
    mysqlxx::Pool::Entry & connection,
    const Context & context,
    std::unordered_map<String, String> tables_dump_queries)
{
    for (const auto & [table_name, dump_query] : tables_dump_queries)
    {
        Context query_context = createQueryContext(context);
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        tryToExecuteQuery(query_prefix + " " + dump_query, query_context, database_name, comment); /// create table.

        auto out = std::make_shared<CountingBlockOutputStream>(getTableOutput(database_name, table_name, query_context));
        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
            out->getHeader(), DEFAULT_BLOCK_SIZE);

        Stopwatch watch;
        copyData(input, *out);
        const Progress & progress = out->getProgress();
        LOG_INFO(
            &Poco::Logger::get("DatabaseMaterializeMySQL(" + database_name + ")"),
            "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec.",
            table_name,
            formatReadableQuantity(progress.written_rows),
            formatReadableSizeWithBinarySuffix(progress.written_bytes),
            watch.elapsedSeconds(),
            formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds())),
            formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
    }
}

void DatabaseMaterializeMySQL::cleanOutdatedTables(const Context & context)
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

void DatabaseMaterializeMySQL::tryDumpTablesData(
    mysqlxx::PoolWithFailover::Entry & connection,
    Context & context,
    bool & opened_transaction)
{
    bool locked_tables = false;
    opened_transaction = false;

    std::unordered_map<String, String> tables_dump_queries;
    try {
        connection->query("FLUSH TABLES;").execute();
        connection->query("FLUSH TABLES WITH READ LOCK;").execute();

        locked_tables = true;
        materialize_metadata = std::make_shared<MaterializeMetadata>(
            connection,
            getDatabase(database_name).getMetadataPath() + "/.metadata",
            checkVariableAndGetVersion(connection));
        connection->query("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;").execute();
        connection->query("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */;").execute();

        opened_transaction = true;
        tables_dump_queries = fetchTablesCreateQuery(connection);

        connection->query("UNLOCK TABLES;").execute();
    } catch (...) {
        if (locked_tables)
            connection->query("UNLOCK TABLES;").execute();

        throw;
    }

    if (!tables_dump_queries.empty()) {
        Position position;
        position.update(materialize_metadata->binlog_position, materialize_metadata->binlog_file, materialize_metadata->executed_gtid_set);

        materialize_metadata->transaction(position, [&]()
        {
            cleanOutdatedTables(context);
            executeDumpQueries(connection, context, tables_dump_queries);
        });

        const auto & position_message = [&]()
        {
            std::stringstream ss;
            position.dump(ss);
            return ss.str();
        };
        LOG_INFO(log, "MySQL dump database position: \n {}", position_message());
    }

    if (opened_transaction) {
        connection->query("COMMIT").execute();
    }
}

void DatabaseMaterializeMySQL::dumpTablesData(Context & context) {
    bool opened_transaction = false;

    int retry_count = 5; // TODO: take from settings
    int max_wait_time_when_mysql_unavailable = 60;

    mysqlxx::PoolWithFailover::Entry connection;

    // TODO: replace retry_count to is_cancelled
    while (retry_count--) {
        try {
            connection = pool.get();
            tryDumpTablesData(connection, context, opened_transaction);
        } catch (...) {
            tryLogCurrentException(log);
            if (opened_transaction) {
                connection->query("ROLLBACK").execute();
            }

            try {
                throw;
            } catch (const mysqlxx::ConnectionFailed &) {
                /// Avoid busy loop when MySQL is not available.
                sleepForMilliseconds(max_wait_time_when_mysql_unavailable);
            }
        }
    }
}

void DatabaseMaterializeMySQL::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    try
    {
        std::unique_lock<std::mutex> lock(mutex);
        nested_database->loadStoredObjects(context, has_force_restore_data_flag, force_attach);
        dumpTablesData(context);

        // TODO: is there a guarantee that dump queries will finish here?
        materialize_thread = std::make_shared<MaterializeMySQLSyncThread>(
            context,
            database_name,
            mysql_database_name,
            pool,
            std::move(client),
            materialize_metadata,
            settings.get());

        materialize_thread->startSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot load MySQL nested database stored objects.");

        if (!force_attach)
            throw;
    }
}

void DatabaseMaterializeMySQL::shutdown()
{
    materialize_thread.reset();

    auto iterator = nested_database->getTablesIterator(global_context, {});

    /// We only shutdown the table, The tables is cleaned up when destructed database
    for (; iterator->isValid(); iterator->next())
        iterator->table()->shutdown();
}

bool DatabaseMaterializeMySQL::empty() const
{
    return nested_database->empty();
}

String DatabaseMaterializeMySQL::getDataPath() const
{
    return nested_database->getDataPath();
}

String DatabaseMaterializeMySQL::getMetadataPath() const
{
    return nested_database->getMetadataPath();
}

String DatabaseMaterializeMySQL::getTableDataPath(const String & table_name) const
{
    return nested_database->getTableDataPath(table_name);
}

String DatabaseMaterializeMySQL::getTableDataPath(const ASTCreateQuery & query) const
{
    return nested_database->getTableDataPath(query);
}

String DatabaseMaterializeMySQL::getObjectMetadataPath(const String & table_name) const
{
    return nested_database->getObjectMetadataPath(table_name);
}

UUID DatabaseMaterializeMySQL::tryGetTableUUID(const String & table_name) const
{
    return nested_database->tryGetTableUUID(table_name);
}

time_t DatabaseMaterializeMySQL::getObjectMetadataModificationTime(const String & name) const
{
    return nested_database->getObjectMetadataModificationTime(name);
}

void DatabaseMaterializeMySQL::createTable(const Context & context, const String & name, const StoragePtr & table, const ASTPtr & query)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support create table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->createTable(context, name, table, query);
}

void DatabaseMaterializeMySQL::dropTable(const Context & context, const String & name, bool no_delay)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support drop table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->dropTable(context, name, no_delay);
}

void DatabaseMaterializeMySQL::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support attach table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->attachTable(name, table, relative_table_path);
}

StoragePtr DatabaseMaterializeMySQL::detachTable(const String & name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support detach table.", ErrorCodes::NOT_IMPLEMENTED);

    return nested_database->detachTable(name);
}

void DatabaseMaterializeMySQL::renameTable(const Context & context, const String & name, IDatabase & to_database, const String & to_name, bool exchange, bool dictionary)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support rename table.", ErrorCodes::NOT_IMPLEMENTED);

    if (exchange)
        throw Exception("MaterializeMySQL database not support exchange table.", ErrorCodes::NOT_IMPLEMENTED);

    if (dictionary)
        throw Exception("MaterializeMySQL database not support rename dictionary.", ErrorCodes::NOT_IMPLEMENTED);

    if (to_database.getDatabaseName() != getDatabaseName())
        throw Exception("Cannot rename with other database for MaterializeMySQL database.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->renameTable(context, name, *nested_database, to_name, exchange, dictionary);
}

void DatabaseMaterializeMySQL::alterTable(const Context & context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
        throw Exception("MaterializeMySQL database not support alter table.", ErrorCodes::NOT_IMPLEMENTED);

    nested_database->alterTable(context, table_id, metadata);
}

bool DatabaseMaterializeMySQL::shouldBeEmptyOnDetach() const
{
    return false;
}

void DatabaseMaterializeMySQL::drop(const Context & context)
{
    if (nested_database->shouldBeEmptyOnDetach())
    {
        for (auto iterator = nested_database->getTablesIterator(context, {}); iterator->isValid(); iterator->next())
        {
            TableExclusiveLockHolder table_lock = iterator->table()->lockExclusively(
                context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

            nested_database->dropTable(context, iterator->name(), true);
        }

        /// Remove metadata info
        Poco::File metadata(getMetadataPath() + "/.metadata");

        if (metadata.exists())
            metadata.remove(false);
    }

    nested_database->drop(context);
}

bool DatabaseMaterializeMySQL::isTableExist(const String & name, const Context & context) const
{
    return nested_database->isTableExist(name, context);
}

StoragePtr DatabaseMaterializeMySQL::tryGetTable(const String & name, const Context & context) const
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        StoragePtr nested_storage = nested_database->tryGetTable(name, context);

        if (!nested_storage)
            return {};

        return std::make_shared<StorageMaterializeMySQL>(std::move(nested_storage), this);
    }

    return nested_database->tryGetTable(name, context);
}

DatabaseTablesIteratorPtr DatabaseMaterializeMySQL::getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name)
{
    if (!MaterializeMySQLSyncThread::isMySQLSyncThread())
    {
        DatabaseTablesIteratorPtr iterator = nested_database->getTablesIterator(context, filter_by_table_name);
        return std::make_unique<DatabaseMaterializeTablesIterator>(std::move(iterator), this);
    }

    return nested_database->getTablesIterator(context, filter_by_table_name);
}

}

#endif
