#include <Databases/DatabaseOverlay.h>

#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Storages/IStorage_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

DatabaseOverlay::DatabaseOverlay(const String & name_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), log(getLogger("DatabaseOverlay(" + name_ + ")"))
{
}

DatabaseOverlay & DatabaseOverlay::registerNextDatabase(DatabasePtr database)
{
    databases.push_back(std::move(database));
    return *this;
}

bool DatabaseOverlay::isTableExist(const String & table_name, ContextPtr context_) const
{
    for (const auto & db : databases)
    {
        if (db->isTableExist(table_name, context_))
            return true;
    }
    return false;
}

StoragePtr DatabaseOverlay::tryGetTable(const String & table_name, ContextPtr context_) const
{
    StoragePtr result = nullptr;
    for (const auto & db : databases)
    {
        result = db->tryGetTable(table_name, context_);
        if (result)
            break;
    }
    return result;
}

void DatabaseOverlay::createTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    for (auto & db : databases)
    {
        if (!db->isReadOnly())
        {
            db->createTable(context_, table_name, table, query);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for CREATE TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::dropTable(ContextPtr context_, const String & table_name, bool sync)
{
    for (auto & db : databases)
    {
        if (db->isTableExist(table_name, context_))
        {
            db->dropTable(context_, table_name, sync);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for DROP TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::attachTable(
    ContextPtr context_, const String & table_name, const StoragePtr & table, const String & relative_table_path)
{
    for (auto & db : databases)
    {
        try
        {
            db->attachTable(context_, table_name, table, relative_table_path);
            return;
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for ATTACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

StoragePtr DatabaseOverlay::detachTable(ContextPtr context_, const String & table_name)
{
    StoragePtr result = nullptr;
    for (auto & db : databases)
    {
        if (db->isTableExist(table_name, context_))
            return db->detachTable(context_, table_name);
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for DETACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::renameTable(
    ContextPtr current_context,
    const String & name,
    IDatabase & to_database,
    const String & to_name,
    bool exchange,
    bool dictionary)
{
    for (auto & db : databases)
    {
        if (db->isTableExist(name, current_context))
        {
            if (DatabaseOverlay * to_overlay_database = typeid_cast<DatabaseOverlay *>(&to_database))
            {
                /// Renaming from Overlay database inside itself or into another Overlay database.
                /// Just use the first database in the overlay as a destination.
                if (to_overlay_database->databases.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The destination Overlay database {} does not have any members", to_database.getDatabaseName());

                db->renameTable(current_context, name, *to_overlay_database->databases[0], to_name, exchange, dictionary);
            }
            else
            {
                /// Renaming into a different type of database. E.g. from Overlay on top of Atomic database into just Atomic database.
                db->renameTable(current_context, name, to_database, to_name, exchange, dictionary);
            }

            return;
        }
    }
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(name));
}

ASTPtr DatabaseOverlay::getCreateTableQueryImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    ASTPtr result = nullptr;
    for (const auto & db : databases)
    {
        result = db->tryGetCreateTableQuery(name, context_);
        if (result)
            break;
    }
    if (!result && throw_on_error)
        throw Exception(
            ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY,
            "There is no metadata of table `{}` in database `{}` (engine {})",
            name,
            getDatabaseName(),
            getEngineName());
    return result;
}

/*
 * DatabaseOverlay cannot be constructed by "CREATE DATABASE" query, as it is not a traditional ClickHouse database
 * To use DatabaseOverlay, it must be constructed programmatically in code
 */
ASTPtr DatabaseOverlay::getCreateDatabaseQuery() const
{
    auto query = std::make_shared<ASTCreateQuery>();
    query->setDatabase(getDatabaseName());
    return query;
}

String DatabaseOverlay::getTableDataPath(const String & table_name) const
{
    String result;
    for (const auto & db : databases)
    {
        result = db->getTableDataPath(table_name);
        if (!result.empty())
            break;
    }
    return result;
}

String DatabaseOverlay::getTableDataPath(const ASTCreateQuery & query) const
{
    String result;
    for (const auto & db : databases)
    {
        result = db->getTableDataPath(query);
        if (!result.empty())
            break;
    }
    return result;
}

UUID DatabaseOverlay::getUUID() const
{
    UUID result = UUIDHelpers::Nil;
    for (const auto & db : databases)
    {
        result = db->getUUID();
        if (result != UUIDHelpers::Nil)
            break;
    }
    return result;
}

UUID DatabaseOverlay::tryGetTableUUID(const String & table_name) const
{
    UUID result = UUIDHelpers::Nil;
    for (const auto & db : databases)
    {
        result = db->tryGetTableUUID(table_name);
        if (result != UUIDHelpers::Nil)
            break;
    }
    return result;
}

void DatabaseOverlay::drop(ContextPtr context_)
{
    for (auto & db : databases)
        db->drop(context_);
}

void DatabaseOverlay::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    for (auto & db : databases)
    {
        if (!db->isReadOnly() && db->isTableExist(table_id.table_name, local_context))
        {
            db->alterTable(local_context, table_id, metadata);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for ALTER TABLE `{}` query in database `{}` (engine {})",
        table_id.table_name,
        getDatabaseName(),
        getEngineName());
}

std::vector<std::pair<ASTPtr, StoragePtr>>
DatabaseOverlay::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    std::vector<std::pair<ASTPtr, StoragePtr>> result;
    for (const auto & db : databases)
    {
        auto db_backup = db->getTablesForBackup(filter, local_context);
        result.insert(result.end(), std::make_move_iterator(db_backup.begin()), std::make_move_iterator(db_backup.end()));
    }
    return result;
}

void DatabaseOverlay::createTableRestoredFromBackup(
    const ASTPtr & create_table_query,
    ContextMutablePtr local_context,
    std::shared_ptr<IRestoreCoordination> /*restore_coordination*/,
    UInt64 /*timeout_ms*/)
{
    /// Creates a tables by executing a "CREATE TABLE" query.
    InterpreterCreateQuery interpreter{create_table_query, local_context};
    interpreter.setInternal(true);
    interpreter.setIsRestoreFromBackup(true);
    interpreter.execute();
}

bool DatabaseOverlay::empty() const
{
    for (const auto & db : databases)
    {
        if (!db->empty())
            return false;
    }
    return true;
}

void DatabaseOverlay::shutdown()
{
    for (auto & db : databases)
        db->shutdown();
}

DatabaseTablesIteratorPtr DatabaseOverlay::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool /*skip_not_loaded*/) const
{
    Tables tables;
    for (const auto & db : databases)
    {
        for (auto table_it = db->getTablesIterator(context_, filter_by_table_name); table_it->isValid(); table_it->next())
            tables.insert({table_it->name(), table_it->table()});
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(tables), getDatabaseName());
}

bool DatabaseOverlay::canContainMergeTreeTables() const
{
    for (const auto & db : databases)
        if (db->canContainMergeTreeTables())
            return true;
    return false;
}

bool DatabaseOverlay::canContainDistributedTables() const
{
    for (const auto & db : databases)
        if (db->canContainDistributedTables())
            return true;
    return false;
}

bool DatabaseOverlay::canContainRocksDBTables() const
{
    for (const auto & db : databases)
        if (db->canContainRocksDBTables())
            return true;
    return false;
}

void DatabaseOverlay::loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadStoredObjects(local_context, mode);
}

bool DatabaseOverlay::supportsLoadingInTopologicalOrder() const
{
    for (const auto & db : databases)
        if (db->supportsLoadingInTopologicalOrder())
            return true;
    return false;
}

void DatabaseOverlay::beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->beforeLoadingMetadata(local_context, mode);
}

void DatabaseOverlay::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadTablesMetadata(local_context, metadata, is_startup);
}

void DatabaseOverlay::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->loadTableFromMetadata(local_context, file_path, name, ast, mode);
            return;
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of loading table `{}` from path `{}` in database `{}` (engine {})",
        name.table,
        file_path,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::loadTableFromMetadataAsync(
    AsyncLoader & async_loader,
    LoadJobSet load_after,
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->loadTableFromMetadataAsync(async_loader, load_after, local_context, file_path, name, ast, mode);
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of loading table `{}` from path `{}` in database `{}` (engine {})",
        name.table,
        file_path,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::startupTableAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    const QualifiedTableName & name,
    LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupTableAsync(async_loader, startup_after, name, mode);
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of starting up table `{}` in database `{}` (engine {})",
        name.table,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::startupDatabaseAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    LoadingStrictnessLevel mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupDatabaseAsync(async_loader, startup_after, mode);
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of starting up asynchronously in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::waitTableStarted(const String & name) const
{
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->waitTableStarted(name);
            return;
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of waiting for table startup `{}` in database `{}` (engine {})",
        name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::waitDatabaseStarted() const
{
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->waitDatabaseStarted();
            return;
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of waiting for startup in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::stopLoading()
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->stopLoading();
            return;
        }
        catch (...)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of stop loading in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::checkMetadataFilenameAvailability(const String & table_name) const
{
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;
        db->checkMetadataFilenameAvailability(table_name);
        return;
    }
}


}
