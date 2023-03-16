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

class MemoryDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit MemoryDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base)
        : DatabaseTablesSnapshotIterator(std::move(base)) {}
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseMemory::DatabaseMemory(const String & name_, UUID uuid, ContextPtr context_)
    : DatabaseWithOwnTablesBase(name_, "DatabaseMemory(" + name_ + ")", context_)
    , data_path("store/")
    , path_to_table_symlinks(fs::path(getContext()->getPath()) / "data" / escapeForFileName(name_) / "")
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
    fs::create_directories(path_to_table_symlinks);
}

String DatabaseMemory::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database {}", table_name, database_name);
    assert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseMemory::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    assert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseMemory::tryCreateSymlink(const String & table_name, const String & actual_data_path, bool if_data_path_exist)
{
    try
    {
        String link = path_to_table_symlinks + escapeForFileName(table_name);
        fs::path data = fs::canonical(getContext()->getPath()) / actual_data_path;
        if (!if_data_path_exist || fs::exists(data))
            fs::create_directory_symlink(data, link);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseMemory::tryRemoveSymlink(const String & table_name)
{
    try
    {
        String path = path_to_table_symlinks + escapeForFileName(table_name);
        fs::remove(path);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

DatabaseTablesIteratorPtr
DatabaseMemory::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name) const
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(local_context, filter_by_table_name);
    return std::make_unique<MemoryDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
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
        auto * create_to_store = query_to_store->as<ASTCreateQuery>();
        if (!create_to_store)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query '{}' is not CREATE query", serializeAST(*query));
        cleanupObjectDefinitionFromTemporaryFlags(*create_to_store);

        const auto & create = query->as<ASTCreateQuery &>();
        assert(table_name == create.getTable());

        auto table_data_path = getTableDataPath(create);
        table_name_to_path.emplace(table_name, table_data_path);

        if (table->storesDataOnDisk())
            tryCreateSymlink(table_name, table_data_path);
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
            assert(getDatabaseName() != DatabaseCatalog::TEMPORARY_DATABASE);
            fs::path table_data_dir{getTableDataPath(table_name)};
            if (fs::exists(table_data_dir))
                fs::remove_all(table_data_dir);
        }
    }
    catch (...)
    {
        std::lock_guard lock{mutex};
        assert(database_name != DatabaseCatalog::TEMPORARY_DATABASE);
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

void DatabaseMemory::renameTable(ContextPtr /*local_context*/, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// TSA does not support conditional locking
{
    if (typeid(*this) != typeid(to_database))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");

    auto & other_db = dynamic_cast<DatabaseMemory &>(to_database);
    bool inside_database = this == &other_db;

    auto detach = [](DatabaseMemory & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        assert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseMemory & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_) TSA_REQUIRES(db.mutex)
    {
        db.tables.emplace(table_name_, table_);
        if (table_data_path_.empty())
            return;
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        if (table_->storesDataOnDisk())
            db.tryCreateSymlink(table_name_, table_data_path_);
    };

    auto assert_can_move_mat_view = [inside_database](const StoragePtr & table_)
    {
        if (inside_database)
            return;
        if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table_.get()))
            if (mv->hasInnerTable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move MaterializedView with inner table to other database");
    };

    String table_data_path;
    String other_table_data_path;

    if (inside_database && table_name == to_table_name)
        return;

    std::unique_lock<std::mutex> db_lock;
    std::unique_lock<std::mutex> other_db_lock;

    if (inside_database)
    {
        db_lock = std::unique_lock{mutex};
    }
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
    StorageID new_table_id = {other_db.database_name, to_table_name, old_table_id.uuid};
    table->checkTableCanBeRenamed({new_table_id});
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    StorageID other_table_new_id = StorageID::createEmpty();
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name);
        if (dictionary && !other_table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Use RENAME/EXCHANGE TABLE (instead of RENAME/EXCHANGE DICTIONARY) for tables");
        other_table_new_id = {database_name, table_name, other_table->getStorageID().uuid};
        other_table->checkTableCanBeRenamed(other_table_new_id);
        assert_can_move_mat_view(other_table);
    }

    table_data_path = detach(*this, table_name, table->storesDataOnDisk());
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

    table->renameInMemory(new_table_id);
    if (exchange)
        other_table->renameInMemory(other_table_new_id);

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(*this, table_name, other_table_data_path, other_table);
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

void DatabaseMemory::drop(ContextPtr)
{
    /// Remove database directory and symlinks to the tables data
    assert(TSA_SUPPRESS_WARNING_FOR_READ(tables).empty());
    try
    {
        fs::remove_all(path_to_table_symlinks);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
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
