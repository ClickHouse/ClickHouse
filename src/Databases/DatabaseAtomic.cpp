#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Poco/File.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Stopwatch.h>
#include <Parsers/formatAST.h>
#include <Common/rename.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_ASSIGN_ALTER;
}

class AtomicDatabaseTablesSnapshotIterator final : public DatabaseTablesSnapshotIterator
{
public:
    explicit AtomicDatabaseTablesSnapshotIterator(DatabaseTablesSnapshotIterator && base)
        : DatabaseTablesSnapshotIterator(std::move(base)) {}
    UUID uuid() const override { return table()->getStorageID().uuid; }
};


DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, "store/", "DatabaseAtomic (" + name_ + ")", context_)
    , path_to_table_symlinks(context_.getPath() + "data/" + escapeForFileName(name_) + "/")
{
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
    std::lock_guard lock(mutex);
    not_in_use = cleenupDetachedTables();
    assertDetachedTableNotInUse(table->getStorageID().uuid);
    DatabaseWithDictionaries::attachTableUnlocked(name, table, relative_table_path);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
    tryCreateSymlink(name, relative_table_path);
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    DetachedTables  not_in_use;
    std::lock_guard lock(mutex);
    auto table = DatabaseWithDictionaries::detachTableUnlocked(name);
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
        std::lock_guard lock(mutex);
        table = getTableUnlocked(table_name);
        table_metadata_path_drop = DatabaseCatalog::instance().getPathForDroppedMetadata(table->getStorageID());
        Poco::File(table_metadata_path).renameTo(table_metadata_path_drop);
        DatabaseWithDictionaries::detachTableUnlocked(table_name);       /// Should never throw
        table_name_to_path.erase(table_name);
    }
    tryRemoveSymlink(table_name);
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

    String old_metadata_path = getObjectMetadataPath(table_name);
    String new_metadata_path = to_database.getObjectMetadataPath(to_table_name);

    auto detach = [this](DatabaseAtomic & db, const String & table_name_)
    {
        auto table_data_path_saved = db.table_name_to_path.find(table_name_)->second;
        db.tables.erase(table_name_);
        db.table_name_to_path.erase(table_name_);
        tryRemoveSymlink(table_name_);
        return table_data_path_saved;
    };

    auto attach = [this](DatabaseAtomic & db, const String & table_name_, const String & table_data_path_, const StoragePtr & table_)
    {
        db.tables.emplace(table_name_, table_);
        db.table_name_to_path.emplace(table_name_, table_data_path_);
        tryCreateSymlink(table_name_, table_data_path_);
    };

    String table_data_path;
    String other_table_data_path;

    bool inside_database = this == &other_db;
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

    StoragePtr table = getTableUnlocked(table_name);
    StoragePtr other_table;
    if (exchange)
        other_table = other_db.getTableUnlocked(to_table_name);

    if (exchange)
        renameExchange(old_metadata_path, new_metadata_path);
    else
        renameNoReplace(old_metadata_path, new_metadata_path);

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
        std::lock_guard lock{mutex};
        not_in_use = cleenupDetachedTables();
        assertDetachedTableNotInUse(query.uuid);
        renameNoReplace(table_metadata_tmp_path, table_metadata_path);
        attachTableUnlocked(query.table, table, table_data_path);   /// Should never throw
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
    SCOPE_EXIT({ Poco::File(table_metadata_tmp_path).remove(); });

    std::lock_guard lock{mutex};
    auto actual_table_id = getTableUnlocked(table_id.table_name)->getStorageID();

    if (table_id.uuid != actual_table_id.uuid)
        throw Exception("Cannot alter table because it was renamed", ErrorCodes::CANNOT_ASSIGN_ALTER);

    renameExchange(table_metadata_tmp_path, table_metadata_path);
}

void DatabaseAtomic::assertDetachedTableNotInUse(const UUID & uuid)
{
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
    return not_in_use;
}

DatabaseTablesIteratorPtr DatabaseAtomic::getTablesIterator(const IDatabase::FilterByNameFunction & filter_by_table_name)
{
    auto base_iter = DatabaseWithOwnTablesBase::getTablesIterator(filter_by_table_name);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
}

DatabaseTablesIteratorPtr DatabaseAtomic::getTablesWithDictionaryTablesIterator(const IDatabase::FilterByNameFunction & filter_by_dictionary_name)
{
    auto base_iter = DatabaseWithDictionaries::getTablesWithDictionaryTablesIterator(filter_by_dictionary_name);
    return std::make_unique<AtomicDatabaseTablesSnapshotIterator>(std::move(typeid_cast<DatabaseTablesSnapshotIterator &>(*base_iter)));
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
        String data = global_context.getPath() + actual_data_path;
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

