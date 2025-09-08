#include <Databases/DatabaseOverlay.h>

#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Storages/IStorage_fwd.h>

#include <Databases/DatabaseFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

DatabaseOverlay::DatabaseOverlay(const String & name_, ContextPtr context_, Mode mode_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), log(getLogger("DatabaseOverlay(" + name_ + ")")), mode(mode_)
{
}

DatabaseOverlay & DatabaseOverlay::registerNextDatabase(DatabasePtr database)
{
    if (!database)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Overlay database {} received a null underlying database pointer",
            backQuote(getDatabaseName()));
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
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). "
            "Run CREATE TABLE in an underlying database (e.g. {}).",
            backQuote(getDatabaseName()),
            databases.empty() ? String("<none>") : databases.front()->getDatabaseName());
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
    if (mode == Mode::FacadeOverCatalog)
        {
            LOG_TRACE(log, "Ignoring DROP TABLE {}.{} in Overlay Database", getDatabaseName(), table_name);
            return;
        }
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
     if (mode == Mode::FacadeOverCatalog)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Overlay Database is read-only; ATTACH not supported");

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
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). "
            "Run RENAME TABLE in an underlying database (e.g. {}).",
            backQuote(getDatabaseName()),
            databases.empty() ? String("<none>") : databases.front()->getDatabaseName());
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

    if (mode == Mode::FacadeOverCatalog)
    {
        auto storage = std::make_shared<ASTStorage>();

        auto engine_func = std::make_shared<ASTFunction>();
        engine_func->name = "Overlay";

        auto args = std::make_shared<ASTExpressionList>();
        args->children.reserve(databases.size());
        for (const auto & db : databases)
            args->children.emplace_back(std::make_shared<ASTLiteral>(db->getDatabaseName()));
        engine_func->arguments = args;

        storage->set(storage->engine, engine_func);
        query->set(query->storage, storage);
    }
    return query;
}

String DatabaseOverlay::getTableDataPath(const String & table_name) const
{
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). Path resolution is not supported here.",
            backQuote(getDatabaseName()));
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
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). Path resolution is not supported here.",
            backQuote(getDatabaseName()));
    String result;
    for (const auto & db : databases)
    {
        result = db->getTableDataPath(query);
        if (!result.empty())
            break;
    }
    return result;
}

bool DatabaseOverlay::isReadOnly() const
{
    // return mode == Mode::FacadeOverCatalog;
    return false;
}

UUID DatabaseOverlay::getUUID() const
{
    if (mode == Mode::OwnedMembers)
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

    return UUIDHelpers::Nil;
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
    if (mode == Mode::FacadeOverCatalog)
        return;
    for (auto & db : databases)
        db->drop(context_);
}

void DatabaseOverlay::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). "
            "Run ALTER TABLE in an underlying database (e.g. {}).",
            backQuote(getDatabaseName()),
            databases.empty() ? String("<none>") : databases.front()->getDatabaseName());
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
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). "
            "Run CREATE TABLE in an underlying database (e.g. {}).",
            backQuote(getDatabaseName()),
            databases.empty() ? String("<none>") : databases.front()->getDatabaseName());
    /// Creates a tables by executing a "CREATE TABLE" query.
    InterpreterCreateQuery interpreter{create_table_query, local_context};
    interpreter.setInternal(true);
    interpreter.setIsRestoreFromBackup(true);
    interpreter.execute();
}

bool DatabaseOverlay::empty() const
{
    if (mode == Mode::FacadeOverCatalog)
        return true;
    for (const auto & db : databases)
        if (!db->empty())
            return false;
    return true;
}

void DatabaseOverlay::shutdown()
{
    for (auto & db : databases)
        db->shutdown();
}

DatabaseTablesIteratorPtr DatabaseOverlay::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    if (mode == Mode::FacadeOverCatalog && skip_not_loaded)
        return std::make_unique<DatabaseTablesSnapshotIterator>(Tables{}, getDatabaseName());
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

void DatabaseOverlay::loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel loading_mode)
{
    if (mode == Mode::FacadeOverCatalog)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadStoredObjects(local_context, loading_mode);
}

bool DatabaseOverlay::supportsLoadingInTopologicalOrder() const
{
    if (this->mode == Mode::FacadeOverCatalog)
        return false;
    for (const auto & db : databases)
        if (db->supportsLoadingInTopologicalOrder())
            return true;
    return false;
}

void DatabaseOverlay::beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel loading_mode)
{
    if (this->mode == Mode::FacadeOverCatalog)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->beforeLoadingMetadata(local_context, loading_mode);
}

void DatabaseOverlay::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    if (this->mode == Mode::FacadeOverCatalog)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadTablesMetadata(local_context, metadata, is_startup);
}

void DatabaseOverlay::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->loadTableFromMetadata(local_context, file_path, name, ast, loading_mode);
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
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->loadTableFromMetadataAsync(async_loader, load_after, local_context, file_path, name, ast, loading_mode);
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
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupTableAsync(async_loader, startup_after, name, loading_mode);
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
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupDatabaseAsync(async_loader, startup_after, loading_mode);
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
    if (mode == Mode::FacadeOverCatalog)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Database {} is an Overlay facade (read-only). Path resolution is not supported here.",
            backQuote(getDatabaseName()));
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;
        db->checkMetadataFilenameAvailability(table_name);
        return;
    }
}

void registerDatabaseOverlay(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        const auto * engine_def = args.create_query.storage;
        const auto * engine = engine_def->engine;
        const String & engine_name = engine->name;

        std::vector<String> sources;

        if (!engine->arguments || engine->arguments->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "{} database requires at least 1 argument: underlying database name(s)", engine_name);

        for (const auto & arg_ast : engine->arguments->children)
        {
            auto lit = evaluateConstantExpressionOrIdentifierAsLiteral(arg_ast, args.context);
            const auto & value = lit->as<ASTLiteral &>().value;
            sources.emplace_back(value.safeGet<String>());
        }

        if (sources.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} database requires at least 1 source database", engine_name);

        std::sort(sources.begin(), sources.end());
        sources.erase(std::unique(sources.begin(), sources.end()), sources.end());

        for (const auto & source_name : sources)
        {
            if (source_name == args.database_name)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} database cannot reference itself: {}", engine_name, source_name);
        }

        auto overlay = std::make_shared<DatabaseOverlay>(args.database_name, args.context, DatabaseOverlay::Mode::FacadeOverCatalog);

        for (const auto & source_name : sources)
        {
            auto source_db = DatabaseCatalog::instance().tryGetDatabase(source_name);
            if (!source_db)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "{} database requires existing underlying database '{}', but it was not found",
                    engine_name, source_name);
            overlay->registerNextDatabase(source_db);
        }

        return overlay;
    };

    factory.registerDatabase("Overlay", create_fn, { .supports_arguments = true });
}


}
