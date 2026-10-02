#include <Storages/System/StatusRequestsPool.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageTableProxy.h>

namespace DB
{

StoragePtr resolveStatusRequestTable(const StorageID & storage_id)
{
    /// The catalog object of a lazily loaded table is a stand-in, not the engine it stands in for, so
    /// without resolving it the caller takes the table for one that "was replaced by an object of
    /// another type" and drops its row. The caller resolved the table to enqueue the request in the
    /// first place, hence `resolveLazyTableIfLoaded`: re-resolution must not be what loads a table.

    /// Resolution by UUID does not depend on the current table name, so it survives renames.
    if (storage_id.hasUUID())
        return resolveLazyTableIfLoaded(DatabaseCatalog::instance().tryGetByUUID(storage_id.uuid).second);

    return resolveLazyTableIfLoaded(
        DatabaseCatalog::instance().tryGetTable(storage_id, Context::getGlobalContextInstance()));
}

DatabasePtr resolveStatusRequestDatabase(const String & database_name)
{
    return DatabaseCatalog::instance().tryGetDatabase(database_name);
}

}
