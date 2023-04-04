#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Parsers/formatAST.h>
#include <Common/atomicRename.h>
#include <Common/filesystemHelpers.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <filesystem>
#include <Interpreters/DDLTask.h>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int DATABASE_NOT_EMPTY;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int INCORRECT_QUERY;
    extern const int ABORTED;
}


DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, UUID uuid, const String & logger_name, ContextPtr context_)
    : DatabaseOrdinary(name_, metadata_path_, "store/", logger_name, context_)
    , TableDataMapping(getContext()->getPath(), "data/" + escapeForFileName(name_), "DatabaseMemory(" + name_ + ")")
    , path_to_metadata_symlink(fs::path(getContext()->getPath()) / "metadata" / escapeForFileName(name_))
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
    fs::create_directories(fs::path(getContext()->getPath()) / "metadata");
    tryCreateMetadataSymlink();
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, UUID uuid, ContextPtr context_)
    : DatabaseAtomic(name_, std::move(metadata_path_), uuid, "DatabaseAtomic (" + name_ + ")", context_)
{
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    return getTableDataPathUnlocked(table_name, database_name);
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    assert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseAtomic::drop(ContextPtr)
{
    assert(TSA_SUPPRESS_WARNING_FOR_READ(tables).empty());
    try
    {
        fs::remove(path_to_metadata_symlink);
        fs::remove_all(path_to_table_symlinks);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
    fs::remove_all(getMetadataPath());
}

void DatabaseAtomic::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::lock_guard lock(mutex);
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    DetachedTables not_in_use;
    std::lock_guard lock(mutex);
    auto table = DatabaseOrdinary::detachTableUnlocked(name);
    table_name_to_path.erase(name);
    detached_tables.emplace(table->getStorageID().uuid, table);
    not_in_use = cleanupDetachedTables();
    return table;
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    auto table = tryGetTable(table_name, local_context);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (table)
        table->dropInnerTableIfAny(sync, local_context);
    else
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(table_name));

    dropTableImpl(local_context, table_name, sync);
}

void DatabaseAtomic::dropTableImpl(ContextPtr local_context, const String & table_name, bool sync)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop;
    StoragePtr table;
    {
        std::lock_guard lock(mutex);
        table = getTableUnlocked(table_name);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());
        auto txn = local_context->getZooKeeperMetadataTransaction();
        if (txn && !local_context->isInternalSubquery())
            txn->commit();      /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// We apply changes in ZooKeeper before applying changes in local metadata file
        /// to reduce probability of failures between these operations
        /// (it's more likely to lost connection, than to fail before applying local changes).
        /// TODO better detection and recovery

        fs::rename(table_metadata_path, table_metadata_path_drop);  /// Mark table as dropped
        DatabaseOrdinary::detachTableUnlocked(table_name);  /// Should never throw
        table_name_to_path.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, table_metadata_path_drop, sync);
}

void DatabaseAtomic::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
    TSA_NO_THREAD_SAFETY_ANALYSIS   /// TSA does not support conditional locking
{
    if (typeid(*this) != typeid(to_database))
    {
        if (typeid_cast<DatabaseOrdinary *>(&to_database))
        {
            /// Allow moving tables between Atomic and Ordinary (with table lock)
            DatabaseOnDisk::renameTable(local_context, table_name, to_database, to_table_name, exchange, dictionary);
            return;
        }

        if (!allowMoveTableToOtherDatabaseEngine(to_database))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Moving tables between databases of different engines is not supported");
    }

    if (exchange && !supportsAtomicRename())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported");

    renameTableImpl<DatabaseAtomic>(local_context, table_name, to_database, to_table_name, exchange, dictionary);
}

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr query_context)
{
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    try
    {
        std::lock_guard lock{mutex};
        if (query.getDatabase() != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                            database_name, query.getDatabase());
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        chassert(DatabaseCatalog::instance().hasUUIDMapping(query.uuid));

        auto txn = query_context->getZooKeeperMetadataTransaction();
        if (txn && !query_context->isInternalSubquery())
            txn->commit();     /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following renameNoReplace(...)
        /// TODO better detection and recovery

        /// It throws if `table_metadata_path` already exists (it's possible if table was detached)
        renameNoReplace(table_metadata_tmp_path, table_metadata_path);  /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        throw;
    }
    if (table->storesDataOnDisk())
        tryCreateSymlink(query.getTable(), table_data_path);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path,
                                      const String & /*statement*/, ContextPtr query_context)
{
    bool check_file_exists = true;
    SCOPE_EXIT({ std::error_code code; if (check_file_exists) std::filesystem::remove(table_metadata_tmp_path, code); });

    std::lock_guard lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot alter table because it was renamed");

    auto txn = query_context->getZooKeeperMetadataTransaction();
    if (txn && !query_context->isInternalSubquery())
        txn->commit();      /// Commit point (a sort of) for Replicated database

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    check_file_exists = renameExchangeIfSupported(table_metadata_tmp_path, table_metadata_path);
    if (!check_file_exists)
        std::filesystem::rename(table_metadata_tmp_path, table_metadata_path);
}

void DatabaseAtomic::assertDetachedTableNotInUse(const UUID & uuid)
{
    /// Without this check the following race is possible since table RWLocks are not used:
    /// 1. INSERT INTO table ...;
    /// 2. DETACH TABLE table; (INSERT still in progress, it holds StoragePtr)
    /// 3. ATTACH TABLE table; (new instance of Storage with the same UUID is created, instances share data on disk)
    /// 4. INSERT INTO table ...; (both Storage instances writes data without any synchronization)
    /// To avoid it, we remember UUIDs of detached tables and does not allow ATTACH table with such UUID until detached instance still in use.
    if (detached_tables.contains(uuid))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Cannot attach table with UUID {}, "
                        "because it was detached but still used by some query. Retry later.", uuid);
}

void DatabaseAtomic::setDetachedTableNotInUseForce(const UUID & uuid)
{
    std::lock_guard lock{mutex};
    detached_tables.erase(uuid);
}

DatabaseAtomic::DetachedTables DatabaseAtomic::cleanupDetachedTables()
{
    DetachedTables not_in_use;
    auto it = detached_tables.begin();
    while (it != detached_tables.end())
    {
        if (it->second.unique())
        {
            not_in_use.emplace(it->first, it->second);
            it = detached_tables.erase(it);
        }
        else
            ++it;
    }
    /// It should be destroyed in caller with released database mutex
    return not_in_use;
}

void DatabaseAtomic::assertCanBeDetached(bool cleanup)
{
    if (cleanup)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock(mutex);
            not_in_use = cleanupDetachedTables();
        }
    }
    std::lock_guard lock(mutex);
    if (!detached_tables.empty())
        throw Exception(ErrorCodes::DATABASE_NOT_EMPTY, "Database {} cannot be detached, because some tables are still in use. "
                        "Retry later.", backQuoteIfNeed(database_name));
}

DatabaseTablesIteratorPtr
DatabaseAtomic::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name) const
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(local_context, filter_by_table_name);
    return std::make_unique<DatabaseTablesSnapshotIteratorWithUUID>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::beforeLoadingMetadata(ContextMutablePtr /*context*/, LoadingStrictnessLevel mode)
{
    if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
        return;

    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    for (const auto & table_path : fs::directory_iterator(path_to_table_symlinks))
    {
        if (!FS::isSymlink(table_path))
        {
            throw Exception(ErrorCodes::ABORTED,
                "'{}' is not a symlink. Atomic database should contains only symlinks.", std::string(table_path.path()));
        }

        fs::remove(table_path);
    }
}

void DatabaseAtomic::loadStoredObjects(
    ContextMutablePtr local_context, LoadingStrictnessLevel mode, bool skip_startup_tables)
{
    beforeLoadingMetadata(local_context, mode);
    DatabaseOrdinary::loadStoredObjects(local_context, mode, skip_startup_tables);
}

void DatabaseAtomic::startupTables(ThreadPool & thread_pool, LoadingStrictnessLevel mode)
{
    DatabaseOrdinary::startupTables(thread_pool, mode);

    if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
        return;

    NameToPathMap table_names;
    {
        std::lock_guard lock{mutex};
        table_names = table_name_to_path;
    }

    fs::create_directories(path_to_table_symlinks);
    for (const auto & table : table_names)
        tryCreateSymlink(table.first, table.second, true);
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    assert(path_to_metadata_symlink != metadata_path);
    fs::path metadata_symlink(path_to_metadata_symlink);
    if (fs::exists(metadata_symlink))
    {
        if (!FS::isSymlink(metadata_symlink))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
            /// fs::exists could return false for broken symlink
            if (FS::isSymlinkNoThrow(metadata_symlink))
                fs::remove(metadata_symlink);
            fs::create_directory_symlink(metadata_path, path_to_metadata_symlink);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void DatabaseAtomic::renameDatabase(ContextPtr query_context, const String & new_name)
{
    /// CREATE, ATTACH, DROP, DETACH and RENAME DATABASE must hold DDLGuard

    bool check_ref_deps = query_context->getSettingsRef().check_referential_table_dependencies;
    bool check_loading_deps = !check_ref_deps && query_context->getSettingsRef().check_table_dependencies;
    if (check_ref_deps || check_loading_deps)
    {
        std::lock_guard lock(mutex);
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first}, check_ref_deps, check_loading_deps);
    }

    try
    {
        fs::remove(path_to_metadata_symlink);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }

    auto new_name_escaped = escapeForFileName(new_name);
    auto old_database_metadata_path = getContext()->getPath() + "metadata/" + escapeForFileName(getDatabaseName()) + ".sql";
    auto new_database_metadata_path = getContext()->getPath() + "metadata/" + new_name_escaped + ".sql";
    renameNoReplace(old_database_metadata_path, new_database_metadata_path);

    String old_path_to_table_symlinks;

    {
        std::lock_guard lock(mutex);
        {
            Strings table_names;
            table_names.reserve(tables.size());
            for (auto & table : tables)
                table_names.push_back(table.first);
            DatabaseCatalog::instance().updateDatabaseName(database_name, new_name, table_names);
        }
        database_name = new_name;

        for (auto & table : tables)
        {
            auto table_id = table.second->getStorageID();
            table_id.database_name = database_name;
            table.second->renameInMemory(table_id);
        }

        path_to_metadata_symlink = getContext()->getPath() + "metadata/" + new_name_escaped;
        old_path_to_table_symlinks = path_to_table_symlinks;
        path_to_table_symlinks = getContext()->getPath() + "data/" + new_name_escaped + "/";
    }

    fs::rename(old_path_to_table_symlinks, path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

void DatabaseAtomic::waitDetachedTableNotInUse(const UUID & uuid)
{
    /// Table is in use while its shared_ptr counter is greater than 1.
    /// We cannot trigger condvar on shared_ptr destruction, so it's busy wait.
    while (true)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            if (!detached_tables.contains(uuid))
                return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void DatabaseAtomic::checkDetachedTableNotInUse(const UUID & uuid)
{
    DetachedTables not_in_use;
    std::lock_guard lock{mutex};
    not_in_use = cleanupDetachedTables();
    assertDetachedTableNotInUse(uuid);
}

}
