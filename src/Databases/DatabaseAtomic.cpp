#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>
#include <Common/renameat2.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <filesystem>


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
}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base)
        : DatabaseTablesSnapshotIterator(std::move(base)) {}
    UUID uuid() const override { return table()->getStorageID().uuid; }
};


DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, UUID uuid, Context & context_)
    : DatabaseOrdinary(name_, std::move(metadata_path_), "store/", "DatabaseAtomic (" + name_ + ")", context_)
    , path_to_table_symlinks(global_context.getPath() + "data/" + escapeForFileName(name_) + "/")
    , path_to_metadata_symlink(global_context.getPath() + "metadata/" + escapeForFileName(name_))
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
    Poco::File(path_to_table_symlinks).createDirectories();
    tryCreateMetadataSymlink();
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

void DatabaseAtomic::drop(const Context &)
{
    assert(tables.empty());
    try
    {
        Poco::File(path_to_metadata_symlink).remove();
        Poco::File(path_to_table_symlinks).remove(true);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(true));
    }
    Poco::File(getMetadataPath()).remove(true);
}

void DatabaseAtomic::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    not_in_use = cleanupDetachedTables();
    auto table_id = table->getStorageID();
    assertDetachedTableNotInUse(table_id.uuid);
    DatabaseWithDictionaries::attachTableUnlocked(name, table, lock);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    auto table = DatabaseWithDictionaries::detachTableUnlocked(name, lock);
    table_name_to_path.erase(name);
    detached_tables.emplace(table->getStorageID().uuid, table);
    not_in_use = cleanupDetachedTables();
    return table;
}

void DatabaseAtomic::dropTable(const Context &, const String & table_name, bool no_delay)
{
    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_metadata_path_drop;
    StoragePtr table;
    {
        std::unique_lock lock(mutex);
        table = getTableUnlocked(table_name, lock);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());
        Poco::File(table_metadata_path).renameTo(table_metadata_path_drop);    /// Mark table as dropped
        DatabaseWithDictionaries::detachTableUnlocked(table_name, lock);       /// Should never throw
        table_name_to_path.erase(table_name);
    }
    if (table->storesDataOnDisk())
        tryRemoveSymlink(table_name);
    /// Remove the inner table (if any) to avoid deadlock
    /// (due to attempt to execute DROP from the worker thread)
    if (auto * mv = dynamic_cast<StorageMaterializedView *>(table.get()))
        mv->dropInnerTable(no_delay);
    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, table_metadata_path_drop, no_delay);
}

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange, bool dictionary)
{
    if (typeid(*this) != typeid(to_database))
    {
        if (!typeid_cast<DatabaseOrdinary *>(&to_database))
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
        /// Allow moving tables between Atomic and Ordinary (with table lock)
        DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name, exchange, dictionary);
        return;
    }

    if (exchange && dictionary)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot exchange dictionaries");

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
        assert(!table_data_path_saved.empty() || db.dictionaries.find(table_name_) != db.dictionaries.end());
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
    else  if (this < &other_db)
    {
        db_lock = std::unique_lock{mutex};
        other_db_lock = std::unique_lock{other_db.mutex};
    }
    else
    {
        other_db_lock = std::unique_lock{other_db.mutex};
        db_lock = std::unique_lock{mutex};
    }

    bool is_dictionary = dictionaries.find(table_name) != dictionaries.end();
    if (exchange && other_db.dictionaries.find(to_table_name) != other_db.dictionaries.end())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot exchange dictionaries");

    if (dictionary != is_dictionary)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Use RENAME DICTIONARY for dictionaries and RENAME TABLE for tables.");

    if (is_dictionary && !inside_database)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot move dictionary to other database");

    StoragePtr table = getTableUnlocked(table_name, db_lock);
    table->checkTableCanBeRenamed();
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name, other_db_lock);
        other_table->checkTableCanBeRenamed();
        assert_can_move_mat_view(other_table);
    }

    /// Table renaming actually begins here
    if (exchange)
        renameExchange(old_metadata_path, new_metadata_path);
    else
        renameNoReplace(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not throw (if them do, it's a logical error)
    table_data_path = detach(*this, table_name, table->storesDataOnDisk());
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name, other_table->storesDataOnDisk());

    auto old_table_id = table->getStorageID();

    table->renameInMemory({other_db.database_name, to_table_name, old_table_id.uuid});
    if (exchange)
        other_table->renameInMemory({database_name, table_name, other_table->getStorageID().uuid});

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(old_table_id.uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(*this, table_name, other_table_data_path, other_table);

    if (is_dictionary)
    {
        auto new_table_id = StorageID(other_db.database_name, to_table_name, old_table_id.uuid);
        renameDictionaryInMemoryUnlocked(old_table_id, new_table_id);
    }
}

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    bool locked_uuid = false;
    try
    {
        std::unique_lock lock{mutex};
        if (query.database != database_name)
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                            database_name, query.database);
        /// Do some checks before renaming file from .tmp to .sql
        not_in_use = cleanupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        /// We will get en exception if some table with the same UUID exists (even if it's detached table or table from another database)
        DatabaseCatalog::instance().addUUIDMapping(query.uuid);
        locked_uuid = true;
        /// It throws if `table_metadata_path` already exists (it's possible if table was detached)
        renameNoReplace(table_metadata_tmp_path, table_metadata_path);  /// Commit point (a sort of)
        attachTableUnlocked(query.table, table, lock);   /// Should never throw
        table_name_to_path.emplace(query.table, table_data_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        if (locked_uuid)
            DatabaseCatalog::instance().removeUUIDMappingFinally(query.uuid);
        throw;
    }
    if (table->storesDataOnDisk())
        tryCreateSymlink(query.table, table_data_path);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    bool check_file_exists = true;
    SCOPE_EXIT({ std::error_code code; if (check_file_exists) std::filesystem::remove(table_metadata_tmp_path, code); });

    std::unique_lock lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name, lock)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception("Cannot alter table because it was renamed", ErrorCodes::CANNOT_ASSIGN_ALTER);

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
    if (detached_tables.count(uuid))
        throw Exception("Cannot attach table with UUID " + toString(uuid) +
              ", because it was detached but still used by some query. Retry later.", ErrorCodes::TABLE_ALREADY_EXISTS);
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

DatabaseTablesIteratorPtr DatabaseAtomic::getTablesIterator(const Context & context, const IDatabase::FilterByNameFunction & filter_by_table_name)
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(context, filter_by_table_name);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

UUID DatabaseAtomic::tryGetTableUUID(const String & table_name) const
{
    if (auto table = tryGetTable(table_name, global_context))
        return table->getStorageID().uuid;
    return UUIDHelpers::Nil;
}

void DatabaseAtomic::loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach)
{
    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    if (has_force_restore_data_flag)
        Poco::File(path_to_table_symlinks).remove(true);

    DatabaseOrdinary::loadStoredObjects(context, has_force_restore_data_flag, force_attach);

    if (has_force_restore_data_flag)
    {
        NameToPathMap table_names;
        {
            std::lock_guard lock{mutex};
            table_names = table_name_to_path;
        }

        Poco::File(path_to_table_symlinks).createDirectories();
        for (const auto & table : table_names)
            tryCreateSymlink(table.first, table.second, true);
    }
}

void DatabaseAtomic::tryCreateSymlink(const String & table_name, const String & actual_data_path, bool if_data_path_exist)
{
    try
    {
        String link = path_to_table_symlinks + escapeForFileName(table_name);
        Poco::File data = Poco::Path(global_context.getPath()).makeAbsolute().toString() + actual_data_path;
        if (!if_data_path_exist || data.exists())
            data.linkTo(link, Poco::File::LINK_SYMBOLIC);
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(true));
    }
}

void DatabaseAtomic::tryRemoveSymlink(const String & table_name)
{
    try
    {
        String path = path_to_table_symlinks + escapeForFileName(table_name);
        Poco::File{path}.remove();
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(true));
    }
}

void DatabaseAtomic::tryCreateMetadataSymlink()
{
    /// Symlinks in data/db_name/ directory and metadata/db_name/ are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    assert(path_to_metadata_symlink != metadata_path);
    Poco::File metadata_symlink(path_to_metadata_symlink);
    if (metadata_symlink.exists())
    {
        if (!metadata_symlink.isLink())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Directory {} exists", path_to_metadata_symlink);
    }
    else
    {
        try
        {
            Poco::File{metadata_path}.linkTo(path_to_metadata_symlink, Poco::File::LINK_SYMBOLIC);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
}

void DatabaseAtomic::renameDatabase(const String & new_name)
{
    /// CREATE, ATTACH, DROP, DETACH and RENAME DATABASE must hold DDLGuard
    try
    {
        Poco::File(path_to_metadata_symlink).remove();
    }
    catch (...)
    {
        LOG_WARNING(log, getCurrentExceptionMessage(true));
    }

    auto new_name_escaped = escapeForFileName(new_name);
    auto old_database_metadata_path = global_context.getPath() + "metadata/" + escapeForFileName(getDatabaseName()) + ".sql";
    auto new_database_metadata_path = global_context.getPath() + "metadata/" + new_name_escaped + ".sql";
    renameNoReplace(old_database_metadata_path, new_database_metadata_path);

    String old_path_to_table_symlinks;

    {
        std::lock_guard lock(mutex);
        DatabaseCatalog::instance().updateDatabaseName(database_name, new_name);
        database_name = new_name;

        for (auto & table : tables)
        {
            auto table_id = table.second->getStorageID();
            table_id.database_name = database_name;
            table.second->renameInMemory(table_id);
        }

        for (auto & dict : dictionaries)
        {
            auto old_name = StorageID(dict.second.create_query);
            auto name = old_name;
            name.database_name = database_name;
            renameDictionaryInMemoryUnlocked(old_name, name);
        }

        path_to_metadata_symlink = global_context.getPath() + "metadata/" + new_name_escaped;
        old_path_to_table_symlinks = path_to_table_symlinks;
        path_to_table_symlinks = global_context.getPath() + "data/" + new_name_escaped + "/";
    }

    Poco::File(old_path_to_table_symlinks).renameTo(path_to_table_symlinks);
    tryCreateMetadataSymlink();
}

void DatabaseAtomic::renameDictionaryInMemoryUnlocked(const StorageID & old_name, const StorageID & new_name)
{
    auto it = dictionaries.find(old_name.table_name);
    assert(it != dictionaries.end());
    assert(it->second.config->getString("dictionary.uuid") == toString(old_name.uuid));
    assert(old_name.uuid == new_name.uuid);
    it->second.config->setString("dictionary.database", new_name.database_name);
    it->second.config->setString("dictionary.name", new_name.table_name);
    auto & create = it->second.create_query->as<ASTCreateQuery &>();
    create.database = new_name.database_name;
    create.table = new_name.table_name;
    assert(create.uuid == new_name.uuid);

    if (old_name.table_name != new_name.table_name)
    {
        auto attach_info = std::move(it->second);
        dictionaries.erase(it);
        dictionaries.emplace(new_name.table_name, std::move(attach_info));
    }

    auto result = external_loader.getLoadResult(toString(old_name.uuid));
    if (!result.object)
        return;
    const auto & dict = dynamic_cast<const IDictionaryBase &>(*result.object);
    dict.updateDictionaryName(new_name);
}
void DatabaseAtomic::waitDetachedTableNotInUse(const UUID & uuid)
{
    {
        std::lock_guard lock{mutex};
        if (detached_tables.count(uuid) == 0)
            return;
    }

    /// Table is in use while its shared_ptr counter is greater than 1.
    /// We cannot trigger condvar on shared_ptr destruction, so it's busy wait.
    while (true)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock{mutex};
            not_in_use = cleanupDetachedTables();
            if (detached_tables.count(uuid) == 0)
                return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

}

