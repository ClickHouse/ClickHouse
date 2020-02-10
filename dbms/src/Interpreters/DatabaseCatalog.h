#pragma once
#include <Storages/IStorage_fwd.h>
#include <Core/UUID.h>
#include <boost/noncopyable.hpp>
#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <array>


namespace DB
{

class Context;
class IDatabase;
struct StorageID;
class Exception;
using DatabasePtr = std::shared_ptr<IDatabase>;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
using Databases = std::map<String, std::shared_ptr<IDatabase>>;

class DatabaseCatalog : boost::noncopyable
{
public:
    static constexpr const char * TEMPORARY_DATABASE = "_temporary_and_external_tables";
    static constexpr const char * SYSTEM_DATABASE = "system";

    static DatabaseCatalog & instance();

    //DatabaseCatalog(/*Context & global_context_, String default_database_*/) {}
    //: global_context(global_context_)/*, default_database(std::move(default_database_))*/ {}

    void loadDatabases();
    void shutdown();


    //static String resolveDatabase(const String & database_name, const String & current_database);
    void assertDatabaseExists(const String & database_name) const;
    void assertDatabaseDoesntExist(const String & database_name) const;

    DatabasePtr getDatabaseForTemporaryTables() const;
    DatabasePtr getSystemDatabase() const;

    void attachDatabase(const String & database_name, const DatabasePtr & database);     // ca, a
    DatabasePtr detachDatabase(const String & database_name);                            // (sr), ca, a

    DatabasePtr getDatabase(const String & database_name, const Context & local_context) const;
    DatabasePtr getDatabase(const String & database_name) const;                         // sr, ca, a
    DatabasePtr tryGetDatabase(const String & database_name) const;                      // sr
    bool isDatabaseExist(const String & database_name) const;                                                           // sr, ca
    Databases getDatabases() const;

    DatabaseAndTable tryGetByUUID(const UUID & uuid) const;

    void assertTableDoesntExist(const StorageID & table_id, const Context & context) const;                             // sr, ca
    bool isTableExist(const StorageID & table_id, const Context & context) const;                                       // sr, ca

    void addUUIDMapping(const UUID & uuid, DatabasePtr database, StoragePtr table);
    void removeUUIDMapping(const UUID & uuid);

    StoragePtr getTable(const StorageID & table_id, const Context & local_context, std::optional<Exception> * exception) const;

private:
    DatabaseCatalog() = default;
    void assertDatabaseExistsUnlocked(const String & database_name) const;
    void assertDatabaseDoesntExistUnlocked(const String & database_name) const;

    struct UUIDToStorageMapPart
    {
        std::unordered_map<UUID, DatabaseAndTable> map;
        mutable std::mutex mutex;
    };

    static constexpr UInt64 bits_for_first_level = 8;
    using UUIDToStorageMap = std::array<UUIDToStorageMapPart, 1ull << bits_for_first_level>;

    inline size_t getFirstLevelIdx(const UUID & uuid) const
    {
        return uuid.toUnderType().low >> (64 - bits_for_first_level);
    }

private:
    //[[maybe_unused]] Context & global_context;
    mutable std::mutex databases_mutex;
    //const String default_database;
    Databases databases;
    UUIDToStorageMap uuid_map;


};


}
