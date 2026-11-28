#include <Storages/System/StatusRequestsPool.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageProxy.h>

namespace DB
{

StoragePtr resolveStatusRequestTable(const StorageID & storage_id)
{
    /// The status is read by casting to the engine, so a lazily loaded table has to be resolved to
    /// the storage behind its proxy. A table with no storage yet has no status to report.
    /// Resolution by UUID does not depend on the current table name, so it survives renames.
    if (storage_id.hasUUID())
        return resolveStorageProxy(DatabaseCatalog::instance().tryGetByUUID(storage_id.uuid).second);

    return resolveStorageProxy(
        DatabaseCatalog::instance().tryGetTable(storage_id, Context::getGlobalContextInstance()));
}

DatabasePtr resolveStatusRequestDatabase(const String & database_name)
{
    return DatabaseCatalog::instance().tryGetDatabase(database_name);
}

}
