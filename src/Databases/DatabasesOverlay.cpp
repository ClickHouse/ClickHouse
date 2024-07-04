#include <Databases/DatabasesOverlay.h>

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

DatabasesOverlay::DatabasesOverlay(const String & name_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), log(getLogger("DatabaseOverlay(" + name_ + ")"))
{
}

DatabasesOverlay & DatabasesOverlay::registerNextDatabase(DatabasePtr database)
{
    databases.push_back(std::move(database));
    return *this;
}

bool DatabasesOverlay::isTableExist(const String & table_name, ContextPtr context_) const
{
    for (const auto & db : databases)
    {
        if (db->isTableExist(table_name, context_))
            return true;
    }
    return false;
}

StoragePtr DatabasesOverlay::tryGetTable(const String & table_name, ContextPtr context_) const
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

void DatabasesOverlay::createTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const ASTPtr & query)
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
        "There is no databases for CREATE TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabasesOverlay::dropTable(ContextPtr context_, const String & table_name, bool sync)
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
        "There is no databases for DROP TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabasesOverlay::attachTable(
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
        "There is no databases for ATTACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

StoragePtr DatabasesOverlay::detachTable(ContextPtr context_, const String & table_name)
{
    StoragePtr result = nullptr;
    for (auto & db : databases)
    {
        if (db->isTableExist(table_name, context_))
            return db->detachTable(context_, table_name);
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There is no databases for DETACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabasesOverlay::renameTable(
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
            if (DatabasesOverlay * to_overlay_database = typeid_cast<DatabasesOverlay *>(&to_database))
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

ASTPtr DatabasesOverlay::getCreateTableQueryImpl(const String & name, ContextPtr context_, bool throw_on_error) const
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
ASTPtr DatabasesOverlay::getCreateDatabaseQuery() const
{
    auto query = std::make_shared<ASTCreateQuery>();
    query->setDatabase(getDatabaseName());
    return query;
}

String DatabasesOverlay::getTableDataPath(const String & table_name) const
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

String DatabasesOverlay::getTableDataPath(const ASTCreateQuery & query) const
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

UUID DatabasesOverlay::getUUID() const
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

UUID DatabasesOverlay::tryGetTableUUID(const String & table_name) const
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

void DatabasesOverlay::drop(ContextPtr context_)
{
    for (auto & db : databases)
        db->drop(context_);
}

void DatabasesOverlay::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
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
        "There is no databases for ALTER TABLE `{}` query in database `{}` (engine {})",
        table_id.table_name,
        getDatabaseName(),
        getEngineName());
}

std::vector<std::pair<ASTPtr, StoragePtr>>
DatabasesOverlay::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    std::vector<std::pair<ASTPtr, StoragePtr>> result;
    for (const auto & db : databases)
    {
        auto db_backup = db->getTablesForBackup(filter, local_context);
        result.insert(result.end(), std::make_move_iterator(db_backup.begin()), std::make_move_iterator(db_backup.end()));
    }
    return result;
}

void DatabasesOverlay::createTableRestoredFromBackup(
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

bool DatabasesOverlay::empty() const
{
    for (const auto & db : databases)
    {
        if (!db->empty())
            return false;
    }
    return true;
}

void DatabasesOverlay::shutdown()
{
    for (auto & db : databases)
        db->shutdown();
}

DatabaseTablesIteratorPtr DatabasesOverlay::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool /*skip_not_loaded*/) const
{
    Tables tables;
    for (const auto & db : databases)
    {
        for (auto table_it = db->getTablesIterator(context_, filter_by_table_name); table_it->isValid(); table_it->next())
            tables.insert({table_it->name(), table_it->table()});
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(tables), getDatabaseName());
}

}
