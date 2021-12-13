#pragma once
#include <Databases/IDatabase.h>

#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

template<typename StorageT, typename... StorageArgs>
void attach(IDatabase & system_database, const String & table_name, StorageArgs && ... args)
{
    if (system_database.getUUID() == UUIDHelpers::Nil)
    {
        /// Attach to Ordinary database
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name);
        system_database.attachTable(table_name, StorageT::create(table_id, std::forward<StorageArgs>(args)...));
    }
    else
    {
        /// Attach to Atomic database
        /// NOTE: UUIDs are not persistent, but it's ok since no data are stored on disk for these storages
        /// and path is actually not used
        auto table_id = StorageID(DatabaseCatalog::SYSTEM_DATABASE, table_name, UUIDHelpers::generateV4());
        String path = "store/" + DatabaseCatalog::getPathForUUID(table_id.uuid);
        system_database.attachTable(table_name, StorageT::create(table_id, std::forward<StorageArgs>(args)...), path);
    }
}

}
