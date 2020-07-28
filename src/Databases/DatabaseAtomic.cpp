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
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int CANNOT_ASSIGN_ALTER;
    extern const int DATABASE_NOT_EMPTY;
    extern const int NOT_IMPLEMENTED;
}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base)
        : DatabaseTablesSnapshotIterator(std::move(base)) {}
    UUID uuid() const override { return table()->getStorageID().uuid; }
};


DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, Context & context_)
    : DatabaseOrdinary(name_, std::move(metadata_path_), "store/", "DatabaseAtomic (" + name_ + ")", context_)
    , path_to_table_symlinks(context_.getPath() + "data/" + escapeForFileName(name_) + "/")
{
    /// Symlinks in data/db_name/ directory are not used by ClickHouse,
    /// it's needed only for convenient introspection.
    Poco::File(path_to_table_symlinks).createDirectories();
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception("Table " + table_name + " not found in database " + getDatabaseName(), ErrorCodes::UNKNOWN_TABLE);
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
    Poco::File(path_to_table_symlinks).remove(true);
    Poco::File(getMetadataPath()).remove(true);
}

void DatabaseAtomic::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    not_in_use = cleenupDetachedTables();
    assertDetachedTableNotInUse(table->getStorageID().uuid);
    DatabaseWithDictionaries::attachTableUnlocked(name, table, lock);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
    tryCreateSymlink(name, relative_table_path);
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    DetachedTables not_in_use;
    std::unique_lock lock(mutex);
    auto table = DatabaseWithDictionaries::detachTableUnlocked(name, lock);
    table_name_to_path.erase(name);
    detached_tables.emplace(table->getStorageID().uuid, table);
    not_in_use = cleenupDetachedTables();
    tryRemoveSymlink(name);
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
    tryRemoveSymlink(table_name);
    /// Notify DatabaseCatalog that table was dropped. It will remove table data in background.
    /// Cleanup is performed outside of database to allow easily DROP DATABASE without waiting for cleanup to complete.
    DatabaseCatalog::instance().enqueueDroppedTableCleanup(table->getStorageID(), table, table_metadata_path_drop, no_delay);
}

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name, bool exchange)
{
    if (typeid(*this) != typeid(to_database))
    {
        if (!typeid_cast<DatabaseOrdinary *>(&to_database))
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
        /// Allow moving tables between Atomic and Ordinary (with table lock)
        DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name, exchange);
        return;
    }
    auto & other_db = dynamic_cast<DatabaseAtomic &>(to_database);
    bool inside_database = this == &other_db;

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [](DatabaseAtomic & db, const String & table_name_)
    {
        auto table_data_path_saved = db.table_name_to_path.find(table_name_)->second;
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        db.tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_)
    {
        db.tables.emplace(table_name_, table_);
        db.table_name_to_path.emplace(table_name_, table_data_path_);
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

    StoragePtr table = getTableUnlocked(table_name, db_lock);
    assert_can_move_mat_view(table);
    StoragePtr other_table;
    if (exchange)
    {
        other_table = other_db.getTableUnlocked(to_table_name, other_db_lock);
        assert_can_move_mat_view(other_table);
    }

    /// Table renaming actually begins here
    if (exchange)
        renameExchange(old_metadata_path, new_metadata_path);
    else
        renameNoReplace(old_metadata_path, new_metadata_path);

    /// After metadata was successfully moved, the following methods should not throw (if them do, it's a logical error)
    table_data_path = detach(*this, table_name);
    if (exchange)
        other_table_data_path = detach(other_db, to_table_name);

    table->renameInMemory({other_db.getDatabaseName(), to_table_name, table->getStorageID().uuid});
    if (exchange)
        other_table->renameInMemory({getDatabaseName(), table_name, other_table->getStorageID().uuid});

    if (!inside_database)
    {
        DatabaseCatalog::instance().updateUUIDMapping(table->getStorageID().uuid, other_db.shared_from_this(), table);
        if (exchange)
            DatabaseCatalog::instance().updateUUIDMapping(other_table->getStorageID().uuid, shared_from_this(), other_table);
    }

    attach(other_db, to_table_name, table_data_path, table);
    if (exchange)
        attach(*this, table_name, other_table_data_path, other_table);
}

void DatabaseAtomic::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    DetachedTables not_in_use;
    auto table_data_path = getTableDataPath(query);
    try
    {
        std::unique_lock lock{mutex};
        not_in_use = cleenupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        renameNoReplace(table_metadata_tmp_path, table_metadata_path);
        attachTableUnlocked(query.table, table, lock);   /// Should never throw
        table_name_to_path.emplace(query.table, table_data_path);
    }
    catch (...)
    {
        Poco::File(table_metadata_tmp_path).remove();
        throw;
    }
    tryCreateSymlink(query.table, table_data_path);
}

void DatabaseAtomic::commitAlterTable(const StorageID & table_id, const String & table_metadata_tmp_path, const String & table_metadata_path)
{
    SCOPE_EXIT({ std::error_code code; std::filesystem::remove(table_metadata_tmp_path, code); });

    std::unique_lock lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name, lock)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception("Cannot alter table because it was renamed", ErrorCodes::CANNOT_ASSIGN_ALTER);

    renameExchange(table_metadata_tmp_path, table_metadata_path);
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
              ", because it was detached but still used by come query. Retry later.", ErrorCodes::TABLE_ALREADY_EXISTS);
}

DatabaseAtomic::DetachedTables DatabaseAtomic::cleenupDetachedTables()
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

void DatabaseAtomic::assertCanBeDetached(bool cleenup)
{
    if (cleenup)
    {
        DetachedTables not_in_use;
        {
            std::lock_guard lock(mutex);
            not_in_use = cleenupDetachedTables();
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

void DatabaseAtomic::loadStoredObjects(Context & context, bool has_force_restore_data_flag)
{
    /// Recreate symlinks to table data dirs in case of force restore, because some of them may be broken
    if (has_force_restore_data_flag)
        Poco::File(path_to_table_symlinks).remove(true);

    DatabaseOrdinary::loadStoredObjects(context, has_force_restore_data_flag);

    if (has_force_restore_data_flag)
    {
        NameToPathMap table_names;
        {
            std::lock_guard lock{mutex};
            table_names = table_name_to_path;
        }
        for (const auto & table : table_names)
            tryCreateSymlink(table.first, table.second);
    }
}

void DatabaseAtomic::tryCreateSymlink(const String & table_name, const String & actual_data_path)
{
    try
    {
        String link = path_to_table_symlinks + escapeForFileName(table_name);
        String data = Poco::Path(global_context.getPath()).makeAbsolute().toString() + actual_data_path;
        Poco::File{data}.linkTo(link, Poco::File::LINK_SYMBOLIC);
    }
    catch (...)
    {
        tryLogCurrentException(log);
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
        tryLogCurrentException(log);
    }
}

}

