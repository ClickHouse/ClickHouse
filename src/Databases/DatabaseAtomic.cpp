#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseReplicated.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Parsers/formatAST.h>
#include <Common/atomicRename.h>
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

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base)
        : DatabaseTablesSnapshotIterator(std::move(base)) {}
    UUID uuid() const override { return table()->getStorageID().uuid; }
};

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, UUID uuid, const String & logger_name, ContextPtr context_)
    : DatabaseOrdinary(name_, metadata_path_, "store/", logger_name, context_)
    , path_to_table_symlinks(fs::path(getContext()->getPath()) / "data" / escapeForFileName(name_) / "")
    , path_to_metadata_symlink(fs::path(getContext()->getPath()) / "metadata" / escapeForFileName(name_))
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
    fs::create_directories(fs::path(getContext()->getPath()) / "metadata");
    fs::create_directories(path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, UUID uuid, ContextPtr context_)
    : DatabaseAtomic(name_, std::move(metadata_path_), uuid, "DatabaseAtomic (" + name_ + ")", context_)
{
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception("Table " + table_name + " not found in database " + database_name, ErrorCodes::UNKNOWN_TABLE);
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
    assert(tables.empty());
    try
    {
        fs::remove(path_to_metadata_symlink);
        fs::remove_all(path_to_table_symlinks);
    }
    catch (...)
    {
        LOG_WARNING(log, fmt::runtime(getCurrentExceptionMessage(true)));
    }
    fs::remove_all(getMetadataPath());
}

void DatabaseAtomic::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseOrdinary::attachTableUnlocked(name, table, lock);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(ContextPtr /* context */, const String & name)
{
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    auto table = DatabaseOrdinary::detachTableUnlocked(name, lock);
    table_name_to_path.erase(name);
    detached_tables.emplace(table->getStorageID().uuid, table);
    not_in_use = cleanupDetachedTables(); //-V1001
    return table;
}

void DatabaseAtomic::dropTable(ContextPtr local_context, const String & table_name, bool no_delay)
{
    auto table = tryGetTable(table_name, local_context);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (table)
        table->dropInnerTableIfAny(no_delay, local_context);
    else
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                        backQuote(database_name), backQuote(table_name));

    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop;
    {
        std::unique_lock lock(mutex);
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
        DatabaseOrdinary::detachTableUnlocked(table_name, lock);  /// Should never throw
        table_name_to_path.erase(table_name);
    }

    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);

    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, table_metadata_path_drop, no_delay);
}

void DatabaseAtomic::renameTable(ContextPtr local_context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
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
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (exchange && !supportsAtomicRename())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RENAME EXCHANGE is not supported");

    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_, bool has_symlink)
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

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_)
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
                throw Exception("Cannot move MaterializedView with inner table to other database", ErrorCodes::NOT_IMPLEMENTED);
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
        other_db.checkMetadataFilenameAvailabilityUnlocked(to_table_name, inside_database ? db_lock : other_db_lock);

    StoragePtr table = getTableUnlocked(table_name, db_lock);

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
        other_table = other_db.getTableUnlocked(to_table_name, other_db_lock);
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

    /// NOTE: replica will be lost if server crashes before the following rename
    /// TODO better detection and recovery

    if (exchange)
        renameExchange(old_metadata_path, new_metadata_path);
    else
        renameNoReplace(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not throw (if them do, it's a logical error)
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
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    bool locked_uuid = false;
    try
    {
        std::unique_lock lock{mutex};
        if (query.getDatabase() != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                            database_name, query.getDatabase());
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        /// We will get en exception if some table with the same UUID exists (even if it's detached table or table from another database)
        DatabaseCatalog::instance().addUUIDMapping(query.uuid);
        locked_uuid = true;

        auto txn = query_context->getZooKeeperMetadataTransaction();
        if (txn && !query_context->isInternalSubquery())
            txn->commit();     /// Commit point (a sort of) for Replicated database

        /// NOTE: replica will be lost if server crashes before the following renameNoReplace(...)
        /// TODO better detection and recovery

        /// It throws if `table_metadata_path` already exists (it's possible if table was detached)
        renameNoReplace(table_metadata_tmp_path, table_metadata_path);  /// Commit point (a sort of)
        attachTableUnlocked(query.getTable(), table, lock);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        fs::remove(table_metadata_tmp_path);
        if (locked_uuid)
            DatabaseCatalog::instance().removeUUIDMappingFinally(query.uuid);
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

    std::unique_lock lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name, lock)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception("Cannot alter table because it was renamed", ErrorCodes::CANNOT_ASSIGN_ALTER);

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
                        "because it was detached but still used by some query. Retry later.", toString(uuid));
}

void DatabaseAtomic::setDetachedTableNotInUseForce(const UUID & uuid)
{
    std::unique_lock lock{mutex};
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
        throw Exception("Database " + backQuoteIfNeed(database_name) + " cannot be detached, "
                        "because some tables are still in use. Retry later.", ErrorCodes::DATABASE_NOT_EMPTY);
}

DatabaseTablesIteratorPtr
DatabaseAtomic::getTablesIterator(ContextPtr local_context, const IDatabase::FilterByNameFunction & filter_by_table_name) const
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(local_context, filter_by_table_name);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, getContext()))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::beforeLoadingMetadata(ContextMutablePtr /*context*/, bool force_restore, bool /*force_attach*/)
{
    if (!force_restore)
        return;

    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    for (const auto & table_path : fs::directory_iterator(path_to_table_symlinks))
    {
        if (!fs::is_symlink(table_path))
        {
            throw Exception(ErrorCodes::ABORTED,
                "'{}' is not a symlink. Atomic database should contains only symlinks.", std::string(table_path.path()));
        }

        fs::remove(table_path);
    }
}

void DatabaseAtomic::loadStoredObjects(
    ContextMutablePtr local_context, bool force_restore, bool force_attach, bool skip_startup_tables)
{
    beforeLoadingMetadata(local_context, force_restore, force_attach);
    DatabaseOrdinary::loadStoredObjects(local_context, force_restore, force_attach, skip_startup_tables);
}

void DatabaseAtomic::startupTables(ThreadPool & thread_pool, bool force_restore, bool force_attach)
{
    DatabaseOrdinary::startupTables(thread_pool, force_restore, force_attach);

    if (!force_restore)
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

void DatabaseAtomic::tryCreateSymlink(const String & table_name, const String & actual_data_path, bool if_data_path_exist)
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
        LOG_WARNING(log, fmt::runtime(getCurrentExceptionMessage(true)));
    }
}

void DatabaseAtomic::tryRemoveSymlink(const String & table_name)
{
    try
    {
        String path = path_to_table_symlinks + escapeForFileName(table_name);
        fs::remove(path);
    }
    catch (...)
    {
        LOG_WARNING(log, fmt::runtime(getCurrentExceptionMessage(true)));
    }
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    assert(path_to_metadata_symlink != metadata_path);
    fs::path metadata_symlink(path_to_metadata_symlink);
    if (fs::exists(metadata_symlink))
    {
        if (!fs::is_symlink(metadata_symlink))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
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

    if (query_context->getSettingsRef().check_table_dependencies)
    {
        std::lock_guard lock(mutex);
        for (auto & table : tables)
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed({database_name, table.first});
    }

    try
    {
        fs::remove(path_to_metadata_symlink);
    }
    catch (...)
    {
        LOG_WARNING(log, fmt::runtime(getCurrentExceptionMessage(true)));
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
