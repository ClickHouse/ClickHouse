#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/DDLLoadingDependencyVisitor.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMaterializedView.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int LOGICAL_ERROR;
    extern const int INCONSISTENT_METADATA_FOR_BACKUP;
    extern const int INCORRECT_QUERY;
}

DatabaseMemory::DatabaseMemory(const String & name_, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context_)
    , data_path("data/" + escapeForFileName(database_name) + "/")
{
    /// Temporary database should not have any data on the moment of its creation
    /// In case of sudden server shutdown remove database folder of temporary database
    if (name_ == DatabaseCatalog::TEMPORARY_DATABASE)
        removeDataPath(context_);
}

void DatabaseMemory::createTable(
    ContextPtr /*context*/,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    std::lock_guard lock{mutex};
    attachTableUnlocked(table_name, table);

    /// Clean the query from temporary flags.
    ASTPtr query_to_store = query;
    if (query)
    {
        query_to_store = query->clone();
        auto * create = query_to_store->as<ASTCreateQuery>();
        if (!create)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", serializeAST(*query));
        cleanupObjectDefinitionFromTemporaryFlags(*create);
    }

    create_queries.emplace(table_name, query_to_store);
}

void DatabaseMemory::dropTable(
    ContextPtr /*context*/,
    const String & table_name,
    bool /*sync*/)
{
    StoragePtr table;
    {
        std::lock_guard lock{mutex};
        table = detachTableUnlocked(table_name);
    }
    try
    {
        /// Remove table without lock since:
        /// - it does not require it
        /// - it may cause lock-order-inversion if underlying storage need to
        ///   resolve tables (like StorageLiveView)
        table->drop();

        if (table->storesDataOnDisk())
        {
            fs::path table_data_dir{fs::path{getContext()->getPath()} / getTableDataPath(table_name)};
            if (fs::exists(table_data_dir))
                fs::remove_all(table_data_dir);
        }
    }
    catch (...)
    {
        std::lock_guard lock{mutex};
        attachTableUnlocked(table_name, table);
        throw;
    }

    std::lock_guard lock{mutex};
    table->is_dropped = true;
    create_queries.erase(table_name);
    UUID table_uuid = table->getStorageID().uuid;
    if (table_uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().removeUUIDMappingFinally(table_uuid);
}

void DatabaseMemory::renameTable(
    ContextPtr /* local_context */,
    const String & table_name,
    IDatabase & to_database,
    const String & to_table_name,
    bool exchange,
    bool dictionary) TSA_NO_THREAD_SAFETY_ANALYSIS /// TSA does not support conditional locking
{
    if (typeid(*this) != typeid(to_database))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");

    if (dictionary)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Dictionaries can be renamed only in Atomic databases");

    auto & other_db = dynamic_cast<DatabaseMemory &>(to_database);
    bool inside_database = this == &other_db;

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_)
    {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move MaterializedView with inner table to other database");
    };

    if (inside_database && table_name == to_table_name)
        return;

    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;
    if (inside_database)
        db_lock = std::unique_lock{mutex};
    else if (this < &other_db)
    {
        db_lock = std::unique_lock{mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{mutex};
    }

    StoragePtr table = getTableUnlocked(table_name);

    if (dictionary && !table->isDictionary())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");

    StorageID old_table_id = table->getStorageID();
    StorageID new_table_id = {other_db.database_name, to_table_name};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table_new_id = {database_name, table_name};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
    }

    ASTPtr old_create_table_query;
    ASTPtr new_create_table_query;
    auto it = create_queries.find(table_name);
    if (it != create_queries.end())
    {
        old_create_table_query = it->second;
        if (old_create_table_query)
        {
            auto & create = old_create_table_query->as<ASTCreateQuery &>();
            create.setDatabase(other_db.database_name);
            create.setTable(to_table_name);
        }
    }

    if (exchange)
    {
        it = other_db.create_queries.find(to_table_name);
        if (it != other_db.create_queries.end())
        {
            new_create_table_query = it->second;
            if (new_create_table_query)
            {
                auto & create = new_create_table_query->as<ASTCreateQuery &>();
                create.setDatabase(database_name);
                create.setTable(table_name);
            }
        }
    }

    tables.erase(table_name);
    create_queries.erase(table_name);

    if (exchange)
    {
        other_db.tables.erase(to_table_name);
        other_db.create_queries.erase(to_table_name);
    }

    table->renameInMemory(new_table_id);
    if (exchange)
        other_table->renameInMemory(other_table_new_id);

    other_db.tables.emplace(to_table_name, table);
    other_db.create_queries.emplace(to_table_name, old_create_table_query);
    if (exchange)
    {
        tables.emplace(table_name, other_table);
        create_queries.emplace(table_name, new_create_table_query);
    }
}

ASTPtr DatabaseMemory::getCreateDatabaseQuery() const
{
    auto create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->set(create_query->storage, std::make_shared<ASTStorage>());
    auto engine = makeASTFunction(getEngineName());
    engine->no_empty_args = true;
    create_query->storage->set(create_query->storage->engine, engine);

    if (const auto comment_value = getDatabaseComment(); !comment_value.empty())
        create_query->set(create_query->comment, std::make_shared<ASTLiteral>(comment_value));

    return create_query;
}

ASTPtr DatabaseMemory::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_name);
    if (it == create_queries.end() || !it->second)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no metadata of table {} in database {}", table_name, database_name);
        else
            return {};
    }
    return it->second->clone();
}

UUID DatabaseMemory::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseMemory::removeDataPath(ContextPtr local_context)
{
    std::filesystem::remove_all(local_context->getPath() + data_path);
}

void DatabaseMemory::drop(ContextPtr local_context)
{
    /// Remove data on explicit DROP DATABASE
    removeDataPath(local_context);
}

void DatabaseMemory::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata)
{
    std::lock_guard lock{mutex};
    auto it = create_queries.find(table_id.table_name);
    if (it == create_queries.end() || !it->second)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot alter: There is no metadata of table {}", table_id.getNameForLogs());

    applyMetadataChangesToCreateQuery(it->second, metadata);

    /// The create query of the table has been just changed, we need to update dependencies too.
    auto ref_dependencies = getDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), it->second);
    auto loading_dependencies = getLoadingDependenciesFromCreateQuery(local_context->getGlobalContext(), table_id.getQualifiedName(), it->second);
    DatabaseCatalog::instance().updateDependencies(table_id, ref_dependencies, loading_dependencies);
}

std::vector<std::pair<ASTPtr, StoragePtr>> DatabaseMemory::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// We need a special processing for the temporary database.
    if (getDatabaseName() != DatabaseCatalog::TEMPORARY_DATABASE)
        return DatabaseWithOwnTablesBase::getTablesForBackup(filter, local_context);

    std::vector<std::pair<ASTPtr, StoragePtr>> res;

    /// `this->tables` for the temporary database doesn't contain real names of tables.
    /// That's why we need to call Context::getExternalTables() and then resolve those names using tryResolveStorageID() below.
    auto external_tables = local_context->getExternalTables();

    for (const auto & [table_name, storage] : external_tables)
    {
        if (!filter(table_name))
            continue;

        auto storage_id = local_context->tryResolveStorageID(StorageID{"", table_name}, Context::ResolveExternal);
        if (!storage_id)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP,
                            "Couldn't resolve the name of temporary table {}", backQuoteIfNeed(table_name));

        /// Here `storage_id.table_name` looks like looks like "_tmp_ab9b15a3-fb43-4670-abec-14a0e9eb70f1"
        /// it's not the real name of the table.
        auto create_table_query = tryGetCreateTableQuery(storage_id.table_name, local_context);
        if (!create_table_query)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP,
                            "Couldn't get a create query for temporary table {}", backQuoteIfNeed(table_name));

        const auto & create = create_table_query->as<const ASTCreateQuery &>();
        if (create.getTable() != table_name)
            throw Exception(ErrorCodes::INCONSISTENT_METADATA_FOR_BACKUP,
                            "Got a create query with unexpected name {} for temporary table {}",
                            backQuoteIfNeed(create.getTable()), backQuoteIfNeed(table_name));

        chassert(storage);
        storage->adjustCreateQueryForBackup(create_table_query);
        res.emplace_back(create_table_query, storage);
    }

    return res;
}

}
