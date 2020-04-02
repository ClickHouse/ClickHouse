#pragma once
#include <Storages/IStorage_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Core/UUID.h>
#include <Poco/Logger.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <map>
#include <set>
#include <unordered_map>
#include <mutex>
#include <array>


namespace DB
{

class Context;
class IDatabase;
class Exception;
class ColumnsDescription;

using DatabasePtr = std::shared_ptr<IDatabase>;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
using Databases = std::map<String, std::shared_ptr<IDatabase>>;

/// Table -> set of table-views that make SELECT from it.
using ViewDependencies = std::map<StorageID, std::set<StorageID>>;
using Dependencies = std::vector<StorageID>;


/// Allows executing DDL query only in one thread.
/// Puts an element into the map, locks tables's mutex, counts how much threads run parallel query on the table,
/// when counter is 0 erases element in the destructor.
/// If the element already exists in the map, waits, when ddl query will be finished in other thread.
class DDLGuard
{
public:
    struct Entry
    {
        std::unique_ptr<std::mutex> mutex;
        UInt32 counter;
    };

    /// Element name -> (mutex, counter).
    /// NOTE: using std::map here (and not std::unordered_map) to avoid iterator invalidation on insertion.
    using Map = std::map<String, Entry>;

    DDLGuard(Map & map_, std::unique_lock<std::mutex> guards_lock_, const String & elem);
    ~DDLGuard();

private:
    Map & map;
    Map::iterator it;
    std::unique_lock<std::mutex> guards_lock;
    std::unique_lock<std::mutex> table_lock;
};


/// Creates temporary table in `_temporary_and_external_tables` with randomly generated unique StorageID.
/// Such table can be accessed from everywhere by its ID.
/// Removes the table from database on destruction.
/// TemporaryTableHolder object can be attached to a query or session Context, so table will be accessible through the context.
struct TemporaryTableHolder : boost::noncopyable
{
    typedef std::function<StoragePtr(const StorageID &)> Creator;

    TemporaryTableHolder() = default;
    TemporaryTableHolder(const Context & context, const Creator & creator, const ASTPtr & query = {});

    /// Creates temporary table with Engine=Memory
    TemporaryTableHolder(const Context & context, const ColumnsDescription & columns, const ASTPtr & query = {});

    TemporaryTableHolder(TemporaryTableHolder && rhs);
    TemporaryTableHolder & operator = (TemporaryTableHolder && rhs);

    ~TemporaryTableHolder();

    StorageID getGlobalTableID() const;

    StoragePtr getTable() const;

    operator bool () const { return id != UUIDHelpers::Nil; }

    const Context * global_context = nullptr;
    IDatabase * temporary_tables = nullptr;
    UUID id = UUIDHelpers::Nil;
};

///TODO maybe remove shared_ptr from here?
using TemporaryTablesMapping = std::map<String, std::shared_ptr<TemporaryTableHolder>>;


class DatabaseCatalog : boost::noncopyable
{
public:
    static constexpr const char * TEMPORARY_DATABASE = "_temporary_and_external_tables";
    static constexpr const char * SYSTEM_DATABASE = "system";

    static DatabaseCatalog & init(const Context * global_context_);
    static DatabaseCatalog & instance();

    void loadDatabases();
    void shutdown();

    /// Get an object that protects the table from concurrently executing multiple DDL operations.
    std::unique_ptr<DDLGuard> getDDLGuard(const String & database, const String & table);


    void assertDatabaseExists(const String & database_name) const;
    void assertDatabaseDoesntExist(const String & database_name) const;

    DatabasePtr getDatabaseForTemporaryTables() const;
    DatabasePtr getSystemDatabase() const;

    void attachDatabase(const String & database_name, const DatabasePtr & database);
    DatabasePtr detachDatabase(const String & database_name, bool drop = false, bool check_empty = true);

    /// database_name must be not empty
    DatabasePtr getDatabase(const String & database_name) const;
    DatabasePtr tryGetDatabase(const String & database_name) const;
    bool isDatabaseExist(const String & database_name) const;
    Databases getDatabases() const;

    /// Same as getDatabase(const String & database_name), but if database_name is empty, current database of local_context is used
    DatabasePtr getDatabase(const String & database_name, const Context & local_context) const;

    /// For all of the following methods database_name in table_id must be not empty (even for temporary tables).
    void assertTableDoesntExist(const StorageID & table_id) const;
    bool isTableExist(const StorageID & table_id) const;
    bool isDictionaryExist(const StorageID & table_id) const;

    StoragePtr getTable(const StorageID & table_id) const;
    StoragePtr tryGetTable(const StorageID & table_id) const;
    DatabaseAndTable getDatabaseAndTable(const StorageID & table_id) const { return getTableImpl(table_id, *global_context); }
    DatabaseAndTable tryGetDatabaseAndTable(const StorageID & table_id) const;
    DatabaseAndTable getTableImpl(const StorageID & table_id, const Context & local_context, std::optional<Exception> * exception = nullptr) const;

    void addDependency(const StorageID & from, const StorageID & where);
    void removeDependency(const StorageID & from, const StorageID & where);
    Dependencies getDependencies(const StorageID & from) const;

    /// For Materialized and Live View
    void updateDependency(const StorageID & old_from, const StorageID & old_where,const StorageID & new_from, const StorageID & new_where);

    /// If table has UUID, addUUIDMapping(...) must be called when table attached to some database
    /// and removeUUIDMapping(...) must be called when it detached.
    /// Such tables can be accessed by persistent UUID instead of database and table name.
    void addUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table);
    void removeUUIDMapping(const UUID & uuid);
    DatabaseAndTable tryGetByUUID(const UUID & uuid) const;

private:
    DatabaseCatalog(const Context * global_context_);
    void assertDatabaseExistsUnlocked(const String & database_name) const;
    void assertDatabaseDoesntExistUnlocked(const String & database_name) const;

    struct UUIDToStorageMapPart
    {
        std::unordered_map<UUID, DatabaseAndTable> map;
        mutable std::mutex mutex;
    };

    static constexpr UInt64 bits_for_first_level = 4;
    using UUIDToStorageMap = std::array<UUIDToStorageMapPart, 1ull << bits_for_first_level>;

    inline size_t getFirstLevelIdx(const UUID & uuid) const
    {
        return uuid.toUnderType().low >> (64 - bits_for_first_level);
    }

private:
    /// For some reason Context is required to get Storage from Database object
    const Context * global_context;
    mutable std::mutex databases_mutex;

    ViewDependencies view_dependencies;

    Databases databases;
    UUIDToStorageMap uuid_map;

    Poco::Logger * log;

    /// Do not allow simultaneous execution of DDL requests on the same table.
    /// database -> object -> (mutex, counter), counter: how many threads are running a query on the table at the same time
    /// For the duration of the operation, an element is placed here, and an object is returned,
    /// which deletes the element in the destructor when counter becomes zero.
    /// In case the element already exists, waits, when query will be executed in other thread. See class DDLGuard below.
    using DDLGuards = std::unordered_map<String, DDLGuard::Map>;
    DDLGuards ddl_guards;
    /// If you capture mutex and ddl_guards_mutex, then you need to grab them strictly in this order.
    mutable std::mutex ddl_guards_mutex;
};

}
