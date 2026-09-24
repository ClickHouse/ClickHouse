#include <Databases/DatabaseOverlay.h>

#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/AsyncLoader.h>
#include <Core/Settings.h>
#include <Databases/DatabaseFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/StorageAlias.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Core/UUID.h>

#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int NOT_IMPLEMENTED;
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
        catch (const std::exception &)
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

DatabaseDetachedTablesSnapshotIteratorPtr DatabaseOverlay::getDetachedTablesIterator(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    SnapshotDetachedTables combined_snapshot;
    for (const auto & db : databases)
    {
        DatabaseDetachedTablesSnapshotIteratorPtr it;
        try
        {
            it = db->getDetachedTablesIterator(context_, filter_by_table_name, skip_not_loaded);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
                continue;
            throw;
        }
        for (; it->isValid(); it->next())
        {
            SnapshotDetachedTable snapshot_table;
            snapshot_table.database = getDatabaseName();
            snapshot_table.table = it->table();
            snapshot_table.uuid = it->uuid();
            snapshot_table.metadata_path = it->metadataPath();
            snapshot_table.is_permanently = it->isPermanently();
            combined_snapshot.emplace(it->table(), std::move(snapshot_table));
        }
    }
    return std::make_unique<DatabaseDetachedTablesSnapshotIterator>(std::move(combined_snapshot));
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
ASTPtr DatabaseOverlay::getCreateDatabaseQueryImpl() const
{
    auto query = make_intrusive<ASTCreateQuery>();
    query->setDatabase(database_name);
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

void DatabaseOverlay::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, const bool validate_new_create_query)
{
    for (auto & db : databases)
    {
        if (!db->isReadOnly() && db->isTableExist(table_id.table_name, local_context))
        {
            db->alterTable(local_context, table_id, metadata, validate_new_create_query);
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

bool DatabaseOverlay::isExternal() const
{
    for (const auto & db : databases)
        if (!db->isExternal())
            return false;
    return true;
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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
        catch (const std::exception &)
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

void DatabaseOverlay::checkTableNameLength(const String & table_name) const
{
    /// The limit belongs to the member createTable writes to, which owns the metadata file.
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;
        db->checkTableNameLength(table_name);
        return;
    }
}


namespace
{

/// Sources are resolved by name on every access, so a source database can be dropped and recreated.
/// A missing source contributes no tables. An `Overlay` source is rejected, because it could form a cycle.
DatabasePtr tryGetOverlaySource(const String & name)
{
    auto database = DatabaseCatalog::instance().tryGetDatabase(name);
    if (typeid_cast<const DatabaseOverlayReadOnly *>(database.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An Overlay database cannot use another Overlay database {} as a source", backQuoteIfNeed(name));
    return database;
}

}

DatabaseOverlayReadOnly::DatabaseOverlayReadOnly(const String & name_, Strings source_databases_, ContextPtr context_)
    : IDatabase(name_), WithContext(context_->getGlobalContext()), source_databases(std::move(source_databases_))
{
}

String DatabaseOverlayReadOnly::findSourceDatabase(const String & table_name, ContextPtr context_) const
{
    for (const auto & source : source_databases)
        if (auto database = tryGetOverlaySource(source); database && database->isTableExist(table_name, context_))
            return source;
    return {};
}

bool DatabaseOverlayReadOnly::isTableExist(const String & table_name, ContextPtr context_) const
{
    return !findSourceDatabase(table_name, context_).empty();
}

StoragePtr DatabaseOverlayReadOnly::tryGetTable(const String & table_name, ContextPtr context_) const
{
    String source = findSourceDatabase(table_name, context_);
    if (source.empty())
        return nullptr;
    return std::make_shared<StorageAlias>(StorageID(getDatabaseName(), table_name), getContext(), source, table_name);
}

DatabaseTablesIteratorPtr DatabaseOverlayReadOnly::getTablesIterator(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool /*skip_not_loaded*/) const
{
    Tables tables;
    for (const auto & source : source_databases)
    {
        auto database = tryGetOverlaySource(source);
        if (!database)
            continue;
        for (auto it = database->getTablesIterator(context_, filter_by_table_name); it->isValid(); it->next())
            if (!tables.contains(it->name()))
                tables.emplace(it->name(), std::make_shared<StorageAlias>(StorageID(getDatabaseName(), it->name()), getContext(), source, it->name()));
    }
    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(tables), getDatabaseName());
}

ASTPtr DatabaseOverlayReadOnly::getCreateTableQueryImpl(const String & table_name, ContextPtr context_, bool throw_on_error) const
{
    String source = findSourceDatabase(table_name, context_);
    if (source.empty())
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(table_name));
        return nullptr;
    }

    const auto & settings = getContext()->getSettingsRef();
    String query = fmt::format("CREATE TABLE {}.{} ENGINE = Alias({}, {})",
        backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(table_name), quoteString(source), quoteString(table_name));
    ParserCreateQuery parser;
    return parseQuery(parser, query, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
}

ASTPtr DatabaseOverlayReadOnly::getCreateDatabaseQueryImpl() const
{
    Strings quoted_sources;
    for (const auto & source : source_databases)
        quoted_sources.push_back(quoteString(source));

    String query = fmt::format("CREATE DATABASE {} ENGINE = Overlay({})", backQuoteIfNeed(database_name), fmt::join(quoted_sources, ", "));
    if (!comment.empty())
        query += " COMMENT " + quoteString(comment);

    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;
    return parseQuery(parser, query, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
}

void registerDatabaseOverlay(DatabaseFactory & factory);
void registerDatabaseOverlay(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        Strings sources;
        for (auto & arg : args.engine_args)
        {
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, args.context);
            sources.push_back(checkAndGetLiteralArgument<String>(arg, "database"));
        }
        if (sources.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Overlay database requires at least one source database");

        if (args.mode <= LoadingStrictnessLevel::CREATE)
            for (const auto & source : sources)
                if (!tryGetOverlaySource(source))
                    throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} doesn't exist", backQuoteIfNeed(source));

        return std::make_shared<DatabaseOverlayReadOnly>(args.database_name, std::move(sources), args.context);
    };

    factory.registerDatabase("Overlay", create_fn, {.supports_arguments = true}, Documentation{
        .description = R"DOCS_MD(
The `Overlay` database engine exposes the union of the tables of several existing databases.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE overlay_db
ENGINE = Overlay(db1[, db2, ...]);
```

A table name is resolved in the source databases in the order they are listed: the first database that has a table with this name wins.

The database owns no tables. Every table of the overlay database behaves as an [`Alias`](/reference/engines/table-engines/special/alias) table to the table of the source database: reading and writing go to the source table.
`CREATE`, `DROP`, `RENAME`, `ATTACH` and `DETACH` of tables inside the overlay database are not supported.

The source databases are resolved by name on every access: when a source database is dropped, its tables disappear from the overlay database, and they reappear when it is created again.
An `Overlay` database cannot be used as a source of another `Overlay` database.

## Access control {#access-control}

As for an `Alias` table, working with a table of the overlay database requires the grants both on the overlay database and on the source table, and the row policies of both apply.
The names of the tables of the source databases are visible to anyone who can list the tables of the overlay database.
)DOCS_MD",
        .syntax = "ENGINE = Overlay(db1[, db2, ...])",
        .examples = {{
            "Combining the tables of two databases",
            R"(
CREATE DATABASE db1;
CREATE DATABASE db2;
CREATE TABLE db1.a (x UInt8) ENGINE = Memory;
CREATE TABLE db2.b (y String) ENGINE = Memory;
INSERT INTO db2.b VALUES ('Hello');
CREATE DATABASE overlay_db ENGINE = Overlay(db1, db2);
SELECT name, engine FROM system.tables WHERE database = 'overlay_db' ORDER BY name;
SELECT * FROM overlay_db.b;
DROP DATABASE overlay_db;
DROP DATABASE db1;
DROP DATABASE db2;
            )",
            R"(
┌─name─┬─engine─┐
│ a    │ Alias  │
│ b    │ Alias  │
└──────┴────────┘
┌─y─────┐
│ Hello │
└───────┘
            )"
        }},
        .introduced_in = {26, 10}});
}

}
