#include <memory>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TableNameHints.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/NamePrompter.h>
#include <Common/quoteString.h>
#include <Common/AsyncLoader.h>
#include <Poco/String.h>


namespace CurrentMetrics
{
    extern const Metric AttachedDatabase;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AMBIGUOUS_IDENTIFIER;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;

    TableNameHints hints(this->shared_from_this(), context);
    /// hint is a pair which holds a single database_name and table_name suggestion for the given table name.
    auto hint = hints.getHintForTable(name);

    if (hint.first.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
    throw Exception(
        ErrorCodes::UNKNOWN_TABLE,
        "Table {}.{} does not exist. Maybe you meant {}.{}?",
        backQuoteIfNeed(getDatabaseName()),
        backQuoteIfNeed(name),
        backQuoteIfNeed(hint.first),
        backQuoteIfNeed(hint.second));
}

String IDatabase::tryResolveTableNameCaseInsensitive(const String & name, ContextPtr context) const
{
    /// Prefer the narrow exact-name lookup first: `getTablesIterator` may enumerate the entire
    /// catalog (remote PostgreSQL / DataLake databases), which is far more expensive than a single
    /// `tryGetTable` probe. The case-insensitive scan only fires when the exact lookup misses.
    /// This is what makes `information_schema.tables` work in standard mode — the database contains
    /// both `tables` and `TABLES`, but a literal lookup of `tables` matches `tables` exactly and
    /// avoids the otherwise-ambiguous case-insensitive scan.
    if (tryGetTable(name, context))
        return name;

    /// `information_schema` (and its uppercase twin) intentionally expose each predefined view in
    /// two cases (`tables` and `TABLES`, etc.). Settings documentation promises those pairs are
    /// canonical aliases of one logical view, so a mixed-case lookup like `TaBlEs` must resolve to
    /// one of them rather than throw on the otherwise-ambiguous case-insensitive scan.
    {
        const String & db_name = getDatabaseName();
        const bool is_info_schema_lower = db_name == "information_schema";
        const bool is_info_schema_upper = db_name == "INFORMATION_SCHEMA";
        if (is_info_schema_lower || is_info_schema_upper)
        {
            static constexpr std::string_view predefined_views[] = {
                "schemata", "tables", "views", "columns",
                "key_column_usage", "referential_constraints", "statistics",
                "character_sets", "collations", "engines",
            };
            const String lowered_name = Poco::toLower(name);
            for (const auto view : predefined_views)
            {
                if (lowered_name != view)
                    continue;
                /// Canonical view name: lowercase variant for the lowercase schema, uppercase for the uppercase one.
                String canonical = is_info_schema_upper ? Poco::toUpper(name) : Poco::toLower(name);
                if (tryGetTable(canonical, context))
                    return canonical;
            }
        }
    }

    /// `getTablesIterator` lists the whole catalog. For remote / data-lake databases
    /// (PostgreSQL, MySQL, DataLake, etc.) that means a paid round trip per missing-name lookup —
    /// far worse than the typo it would help with. Skip the scan for those engines; the
    /// exact-name `tryGetTable` above already covered the cheap path.
    if (isRemoteDatabase())
        return {};

    String found_name;
    for (auto table_it = getTablesIterator(context); table_it->isValid(); table_it->next())
    {
        const auto & table_name = table_it->name();
        if (Poco::icompare(table_name, name) != 0)
            continue;
        if (!found_name.empty())
        {
            throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                "Table name '{}' is ambiguous: matches multiple tables with different cases: '{}' and '{}'",
                name, found_name, table_name);
        }
        found_name = table_name;
    }
    return found_name;
}

IDatabase::IDatabase(String database_name_) : database_name(std::move(database_name_))
{
    CurrentMetrics::add(CurrentMetrics::AttachedDatabase, 1);
}

IDatabase::~IDatabase()
{
    CurrentMetrics::sub(CurrentMetrics::AttachedDatabase, 1);
}

void IDatabase::alterDatabaseComment(const AlterCommand & /*command*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: ALTER DATABASE COMMENT is not supported", getEngineName());
}

std::vector<std::pair<ASTPtr, StoragePtr>> IDatabase::getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const
{
    /// Cannot backup any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup tables in database {}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()));
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

void IDatabase::loadTablesMetadata(ContextPtr /*local_context*/, ParsedTablesMetadata & /*metadata*/, bool /*is_startup*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

void IDatabase::loadTableFromMetadata(
    ContextMutablePtr /*local_context*/,
    const String & /*file_path*/,
    const QualifiedTableName & /*name*/,
    const ASTPtr & /*ast*/,
    LoadingStrictnessLevel /*mode*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

LoadTaskPtr IDatabase::loadTableFromMetadataAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*load_after*/,
    ContextMutablePtr /*local_context*/,
    const String & /*file_path*/,
    const QualifiedTableName & /*name*/,
    const ASTPtr & /*ast*/,
    LoadingStrictnessLevel /*mode*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

LoadTaskPtr IDatabase::startupTableAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*startup_after*/,
    const QualifiedTableName & /*name*/,
    LoadingStrictnessLevel /*mode*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

LoadTaskPtr IDatabase::startupDatabaseAsync(
    AsyncLoader & /*async_loader*/,
    LoadJobSet /*startup_after*/,
    LoadingStrictnessLevel /*mode*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

DatabaseDetachedTablesSnapshotIteratorPtr IDatabase::getDetachedTablesIterator(
    ContextPtr /*context*/, const FilterByNameFunction & /*filter_by_table_name = {}*/, bool /*skip_not_loaded = false*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get detached tables for Database{}", getEngineName());
}

void IDatabase::createTable(
    ContextPtr /*context*/,
    const String & /*name*/,
    const StoragePtr & /*table*/,
    const ASTPtr & /*query*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no CREATE TABLE query for Database{}", getEngineName());
}

void IDatabase::dropTable( /// NOLINT
    ContextPtr /*context*/,
    const String & /*name*/,
    [[maybe_unused]] bool sync)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DROP TABLE query for Database{}", getEngineName());
}

void IDatabase::attachTable(ContextPtr /* context */, const String & /*name*/, const StoragePtr & /*table*/, [[maybe_unused]] const String & relative_table_path) /// NOLINT
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no ATTACH TABLE query for Database{}", getEngineName());
}

StoragePtr IDatabase::detachTable(ContextPtr /* context */, const String & /*name*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DETACH TABLE query for Database{}", getEngineName());
}

void IDatabase::detachTablePermanently(ContextPtr /*context*/, const String & /*name*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There is no DETACH TABLE PERMANENTLY query for Database{}", getEngineName());
}

Strings IDatabase::getNamesOfPermanentlyDetachedTables() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot get names of permanently detached tables for Database{}", getEngineName());
}

void IDatabase::renameTable(
    ContextPtr /*context*/,
    const String & /*name*/,
    IDatabase & /*to_database*/,
    const String & /*to_name*/,
    bool /*exchange*/,
    bool /*dictionary*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: renameTable() is not supported", getEngineName());
}

void IDatabase::alterTable(
    ContextPtr /*context*/,
    const StorageID & /*table_id*/,
    const StorageInMemoryMetadata & /*metadata*/,
    const bool /*validate_new_create_query*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: alterTable() is not supported", getEngineName());
}

void IDatabase::renameDatabase(ContextPtr, const String & /*new_name*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{}: RENAME DATABASE is not supported", getEngineName());
}

void IDatabase::applySettingsChanges(const SettingsChanges &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Database engine {} either does not support settings, or does not support altering settings",
                    getEngineName());
}

void IDatabase::stopReplication()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database engine {} does not run a replication thread", getEngineName());
}

BlockIO IDatabase::tryEnqueueReplicatedDDL(const ASTPtr & /*query*/, ContextPtr /*query_context*/, [[maybe_unused]] QueryFlags flags, DDLGuardPtr && /*database_guard*/) /// NOLINT
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Database engine {} does not have replicated DDL queue", getEngineName());
}

ASTPtr IDatabase::getCreateTableQueryImpl(const String & /*name*/, ContextPtr /*context*/, bool throw_on_error) const
{
    if (throw_on_error)
        throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "There is no SHOW CREATE TABLE query for Database{}", getEngineName());
    return nullptr;
}

DiskPtr IDatabase::getDisk() const
{
    return Context::getGlobalContextInstance()->getDatabaseDisk();
}
}
