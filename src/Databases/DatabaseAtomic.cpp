#include <filesystem>
#include <Core/Settings.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseMetadataDiskSettings.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Storages/StorageMaterializedView.h>
#include <base/isSharedPtrUnique.h>
#include <Common/PoolId.h>
#include <Common/atomicRename.h>
#include <Common/logger_useful.h>


namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsBool check_table_dependencies;
}

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
    extern const int LOGICAL_ERROR;
}


namespace DatabaseMetadataDiskSetting
{
extern const DatabaseMetadataDiskSettingsString disk;
}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base) noexcept
        : DatabaseTablesSnapshotIterator(std::move(base))
    {
    }
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseAtomic::DatabaseAtomic(
    String name_,
    String metadata_path_,
    UUID uuid,
    const String & logger_name,
    ContextPtr context_,
    DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseOrdinary(name_, metadata_path_, "store/", logger_name, context_, database_metadata_disk_settings_)
    , path_to_table_symlinks(fs::path("data") / escapeForFileName(name_) / "")
    , path_to_metadata_symlink(fs::path("metadata") / escapeForFileName(name_))
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
}

DatabaseAtomic::DatabaseAtomic(
    String name_, String metadata_path_, UUID uuid, ContextPtr context_, DatabaseMetadataDiskSettings database_metadata_disk_settings_)
    : DatabaseAtomic(name_, std::move(metadata_path_), uuid, "DatabaseAtomic (" + name_ + ")", context_, database_metadata_disk_settings_)
{
}

void DatabaseAtomic::createDirectories()
{
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
}

void DatabaseAtomic::createDirectoriesUnlocked()
{
    auto db_disk = getDisk();

    DatabaseOnDisk::createDirectoriesUnlocked();
    db_disk->createDirectories("metadata");
    if (db_disk->isSymlinkSupported())
        db_disk->createDirectories(path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database {}", table_name, database_name);
    assert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
    assert(tmp != data_path && !tmp.empty());
    return tmp;
}

void DatabaseAtomic::drop(ContextPtr)
{
    waitDatabaseStarted();
    assert(TSA_SUPPRESS_WARNING_FOR_READ(tables).empty());

    auto db_disk = getDisk();
    try
    {
        if (db_disk->isSymlinkSupported() && !db_disk->isReadOnly())
        {
            db_disk->removeFileIfExists(path_to_metadata_symlink);
            db_disk->removeRecursive(path_to_table_symlinks);
        }
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
    if (!db_disk->isReadOnly())
        db_disk->removeRecursive(getMetadataPath());
}

void DatabaseAtomic::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::lock_guard lock(mutex);
    createDirectoriesUnlocked();
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    // it is important to call the destructors of not_in_use without
    // locked mutex to avoid potential deadlock.
    DetachedTables not_in_use;
    StoragePtr detached_table;
    {
        std::lock_guard lock(mutex);
        detached_table = DatabaseOrdinary::detachTableUnlocked(name);
        table_name_to_path.erase(name);
        detached_tables.emplace(detached_table->getStorageID().uuid, detached_table);
        not_in_use = cleanupDetachedTables();
    }

    if (!not_in_use.empty())
    {
        not_in_use.clear();
        LOG_DEBUG(log, "Finished removing not used detached tables");
    }

    return detached_table;
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool sync)
{
    waitDatabaseStarted();
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
    auto db_disk = getDisk();
    {
        std::lock_guard lock(mutex);
        table = getTableUnlocked(table_name);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());

        db_disk->createDirectories(fs::path(table_metadata_path_drop).parent_path());

        auto txn = local_context->getZooKeeperMetadataTransaction();
        if (txn && !local_context->isInternalSubquery())
            txn->commit();      /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following rename
        /// We apply changes in ZooKeeper before applying changes in local metadata file
        /// to reduce probability of failures between these operations
        /// (it's more likely to lost connection, than to fail before applying local changes).
        /// TODO better detection and recovery

        db_disk->replaceFile(table_metadata_path, table_metadata_path_drop); /// Mark table as dropped
        DatabaseOrdinary::detachTableUnlocked(table_name);  /// Should never throw
        table_name_to_path.erase(table_name);
        snapshot_detached_tables.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, db_disk, table_metadata_path_drop, sync);
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

    std::string message;
    if (exchange && !supportsAtomicRename(&message))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported because exchanging files is not supported by the OS ({})", message);

    createDirectories();
    waitDatabaseStarted();

    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;

    if (!inside_database)
        other_db.createDirectories();

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_, bool has_symlink) TSA_REQUIRES(db.mutex)
    {
        auto it = db.table_name_to_path.find(table_name_);
        String table_data_path_saved;
        /// Path can be not set for DDL dictionaries, but it does not matter for StorageDictionary.
        if (it != db.table_name_to_path.end())
            table_data_path_saved = it->second;
        assert(!table_data_path_saved.empty());
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        if (has_symlink)
            db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_) TSA_REQUIRES(db.mutex)
    {
        db.tables.emplace(table_name_, table_);
        if (table_data_path_.empty())
            return;
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        if (table_->storesDataOnDisk())
            db.tryCreateSymlink(table_);
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

    if (!exchange)
        other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name);

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

    /// Table renaming actually begins here
    auto txn = local_context->getZooKeeperMetadataTransaction();
    if (txn && !local_context->isInternalSubquery())
        txn->commit();     /// Commit point (a sort of) for Replicated database

    auto db_disk = getDisk();

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery
    if (exchange)
        db_disk->renameExchange(old_metadata_path, new_metadata_path);
    else
        db_disk->moveFile(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not throw (if they do, it's a logical error)
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

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path,
                                       ContextPtr query_context)
{
    auto db_disk = getDisk();

    createDirectories();
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
        db_disk->moveFile(table_metadata_tmp_path, table_metadata_path); /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        db_disk->removeFileIfExists(table_metadata_tmp_path);
        throw;
    }
    if (table->storesDataOnDisk())
        tryCreateSymlink(table);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path,
                                      const String & /*statement*/, ContextPtr query_context)
{
    auto db_disk = getDisk();

    bool check_file_exists = true;
    SCOPE_EXIT({
        if (check_file_exists)
            db_disk->removeFileIfExists(table_metadata_tmp_path);
    });

    std::lock_guard lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception(ErrorCodes::CANNOT_ASSIGN_ALTER, "Cannot alter table because it was renamed");

    auto txn = query_context->getZooKeeperMetadataTransaction();
    if (txn && !query_context->isInternalSubquery())
        txn->commit();      /// Commit point (a sort of) for Replicated database

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    check_file_exists = db_disk->renameExchangeIfSupported(table_metadata_tmp_path, table_metadata_path);
    if (!check_file_exists)
        db_disk->replaceFile(table_metadata_tmp_path, table_metadata_path);
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
    if (detached_tables.empty())
        return not_in_use;
    auto it = detached_tables.begin();
    LOG_DEBUG(log, "There are {} detached tables. Start searching non used tables.", detached_tables.size());
    while (it != detached_tables.end())
    {
        if (isSharedPtrUnique(it->second))
        {
            not_in_use.emplace(it->first, it->second);
            it = detached_tables.erase(it);
        }
        else
            ++it;
    }
    LOG_DEBUG(log, "Found {} non used tables in detached tables.", not_in_use.size());
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
DatabaseAtomic::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    auto base_iter = DatabaseOrdinary::getTablesIterator(local_context, filter_by_table_name, skip_not_loaded);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::beforeLoadingMetadata(ContextMutablePtr /*context*/, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
        return;

    if (!db_disk->isSymlinkSupported())
        return;

    // When `db_disk` is a `DiskLocal` object, `existsDirectory` will return false if the input path is a symlink.
    // So we use `existsFileOrDirectory` here to check if the symlink exists.
    if (!db_disk->existsFileOrDirectory(path_to_table_symlinks))
        return;

    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    for (const auto it = db_disk->iterateDirectory(path_to_table_symlinks); it->isValid(); it->next())
    {
        auto table_path = fs::path(it->path());
        if (table_path.filename().empty())
            table_path = table_path.parent_path();
        if (!db_disk->isSymlink(table_path))
        {
            throw Exception(
                ErrorCodes::ABORTED, "'{}' is not a symlink. Atomic database should contains only symlinks.", std::string(table_path));
        }

        db_disk->removeFileIfExists(table_path);
    }
}

LoadTaskPtr DatabaseAtomic::startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode)
{
    auto db_disk = getDisk();

    auto base = DatabaseOrdinary::startupDatabaseAsync(async_loader, std::move(startup_after), mode);
    auto job = makeLoadJob(
        base->goals(),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Atomic database {}", getDatabaseName()),
        [this, mode, db_disk](AsyncLoader &, const LoadJobPtr &)
        {
            if (mode < LoadingStrictnessLevel::FORCE_RESTORE)
                return;
            NameToPathMap table_names;
            {
                std::lock_guard lock{mutex};
                table_names = table_name_to_path;
            }
            if (db_disk->isSymlinkSupported())
                db_disk->createDirectories(path_to_table_symlinks);
            for (const auto & table : table_names)
            {
                /// All tables in database should be loaded at this point
                StoragePtr table_ptr = tryGetTable(table.first, getContext());
                if (table_ptr)
                {
                    if (table_ptr->storesDataOnDisk())
                        tryCreateSymlink(table_ptr, true);
                }
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is not loaded before database startup", table.first);
            }
        });
    std::scoped_lock lock(mutex);
    return startup_atomic_database_task = makeLoadTask(async_loader, {job});
}

void DatabaseAtomic::waitDatabaseStarted() const
{
    LoadTaskPtr task;
    {
        std::scoped_lock lock(mutex);
        task = startup_atomic_database_task;
    }
    if (task)
        waitLoad(currentPoolOr(TablesLoaderForegroundPoolId), task, false);
}

void DatabaseAtomic::stopLoading()
{
    LoadTaskPtr stop_atomic_database;
    {
        std::scoped_lock lock(mutex);
        stop_atomic_database.swap(startup_atomic_database_task);
    }
    stop_atomic_database.reset();
    DatabaseOrdinary::stopLoading();
}

void DatabaseAtomic::tryCreateSymlink(const StoragePtr & table, bool if_data_path_exist)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    if (table->getDataPaths().empty())
        return;

    const auto table_data_path = fs::path(table->getDataPaths().front()).lexically_normal();

    try
    {
        String table_name = table->getStorageID().getTableName();

        if (!table->storesDataOnDisk())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} doesn't have data path to create symlink", table_name);

        String link = path_to_table_symlinks / escapeForFileName(table_name);

        LOG_DEBUG(
            log,
            "Trying to create a symlink for table {}, data_path {}, link {}",
            table->getStorageID().getNameForLogs(),
            table_data_path,
            link);

        /// If it already points where needed.
        if (db_disk->equivalentNoThrow(table_data_path, link))
            return;

        if (if_data_path_exist && !db_disk->existsFileOrDirectory(data_path))
            return;

        db_disk->createDirectorySymlink(table_data_path, link);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryRemoveSymlink(const String & table_name)
{
    auto db_disk = getDisk();

    if (!db_disk->isSymlinkSupported())
        return;

    try
    {
        String path = path_to_table_symlinks / escapeForFileName(table_name);
        db_disk->removeFileIfExists(path);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    auto db_disk = getDisk();
    if (!db_disk->isSymlinkSupported())
        return;

    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    chassert(path_to_metadata_symlink != metadata_path);
    if (db_disk->existsFileOrDirectory(path_to_metadata_symlink))
    {
        if (!db_disk->isSymlink(path_to_metadata_symlink))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} already exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
            /// fs::exists could return false for broken symlink
            if (db_disk->isSymlinkNoThrow(path_to_metadata_symlink))
                db_disk->removeFileIfExists(path_to_metadata_symlink);

            LOG_DEBUG(
                log,
                "Creating directory symlink, path_to_metadata_symlink: {}, metadata_path: {}",
                path_to_metadata_symlink,
                metadata_path);

            db_disk->createDirectorySymlink(metadata_path, path_to_metadata_symlink);
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
    createDirectories();
    waitDatabaseStarted();

    bool check_ref_deps = query_context->getSettingsRef()[Setting::check_referential_table_dependencies];
    bool check_loading_deps = !check_ref_deps && query_context->getSettingsRef()[Setting::check_table_dependencies];
    if (check_ref_deps || check_loading_deps)
    {
        std::lock_guard lock(mutex);
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first}, check_ref_deps, check_loading_deps);
    }


    try
    {
        auto db_disk = getDisk();
        if (db_disk->isSymlinkSupported())
            db_disk->removeFileIfExists(path_to_metadata_symlink);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
    }

    auto new_name_escaped = escapeForFileName(new_name);
    auto old_database_metadata_path = fs::path("metadata") / (escapeForFileName(getDatabaseName()) + ".sql");
    auto new_database_metadata_path = fs::path("metadata") / (new_name_escaped + ".sql");
    auto default_db_disk = getContext()->getDatabaseDisk();
    default_db_disk->moveFile(old_database_metadata_path, new_database_metadata_path);

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

        for (auto & [detached_table_name, snapshot] : snapshot_detached_tables)
        {
            snapshot.database = database_name;
        }

        path_to_metadata_symlink = fs::path("metadata") / new_name_escaped;
        old_path_to_table_symlinks = path_to_table_symlinks;
        path_to_table_symlinks = fs::path("data") / new_name_escaped / "";
    }

    auto db_disk = getDisk();
    if (db_disk->isSymlinkSupported())
    {
        db_disk->moveDirectory(old_path_to_table_symlinks, path_to_table_symlinks);
        tryCreateMetadataSymlink();
    }
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

void registerDatabaseAtomic(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        if (args.database_name.ends_with(DatabaseReplicated::BROKEN_REPLICATED_TABLES_SUFFIX))
            args.context->addOrUpdateWarningMessage(
                Context::WarningType::MAYBE_BROKEN_TABLES,
                PreformattedMessage::create(
                    "The database {} is probably created during recovering a lost replica. If it has no tables, it can be deleted. If it "
                    "has tables, it worth to check why they were considered broken.",
                    backQuoteIfNeed(args.database_name)));

        DatabaseMetadataDiskSettings database_metadata_disk_settings;
        auto * engine_define = args.create_query.storage;
        chassert(engine_define);
        database_metadata_disk_settings.loadFromQuery(*engine_define, args.context, args.create_query.attach);

        return make_shared<DatabaseAtomic>(
            args.database_name, args.metadata_path, args.uuid, args.context, database_metadata_disk_settings);
    };
    factory.registerDatabase("Atomic", create_fn, /*features=*/{.supports_settings = true});
}

}
