#pragma once
#include <Databases/IDatabase.h>
#include <Databases/DatabasesCommon.h>

#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

template<typename StorageT, typename... StorageArgs>
void attachLazy(ContextPtr context, IDatabase & system_database, const String & table_name, StorageArgs && ... args)
{
    assert(system_database.getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);
    /// TODO: move lazy interface into IDatabase
    auto & db = dynamic_cast<DatabaseWithOwnTablesBase &>(system_database);
    if (system_database.getUUID() == UUIDHelpers::Nil)
    {
        /// Attach to Ordinary database.
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
        db.registerLazyTable(context, table_name, [table_id, ... captured_args = std::forward<StorageArgs>(args)] mutable
        {
            return std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(captured_args)...);
        });
    }
    else
    {
        /// Attach to Atomic database.
        /// NOTE: UUIDs are not persistent, but it's ok since no data are stored on disk for these storages
        /// and path is actually not used
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name, UUIDHelpers::generateV4());
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid);
        String path = "store/" + DatabaseCatalog::getPathForUUID(table_id.uuid);
        db.registerLazyTable(context, table_name, [table_id, ... captured_args = std::forward<StorageArgs>(args)] mutable
        {
            return std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(captured_args)...);
        }, path);
    }
}

template<typename StorageT, typename... StorageArgs>
void attach(ContextPtr context, IDatabase & system_database, const String & table_name, StorageArgs && ... args)
{
    assert(system_database.getDatabaseName() == DatabaseCatalog::SYSTEM_DATABASE);
    if (system_database.getUUID() == UUIDHelpers::Nil)
    {
        /// Attach to Ordinary database.
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
        system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(args)...));
    }
    else
    {
        /// Attach to Atomic database.
        /// NOTE: UUIDs are not persistent, but it's ok since no data are stored on disk for these storages
        /// and path is actually not used
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name, UUIDHelpers::generateV4());
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid);
        String path = "store/" + DatabaseCatalog::getPathForUUID(table_id.uuid);
        system_database.attachTable(context, table_name, std::make_shared<StorageT>(table_id, std::forward<StorageArgs>(args)...), path);
    }
}

}
