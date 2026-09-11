#include <Storages/System/StatusRequestsPool.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

namespace DB
{

StoragePtr resolveStatusRequestTable(const StorageID & storage_id)
{
    /// Resolution by UUID does not depend on the current table name, so it survives renames.
    if (storage_id.hasUUID())
        return DatabaseCatalog::instance().tryGetByUUID(storage_id.uuid).second;

    return DatabaseCatalog::instance().tryGetTable(storage_id, Context::getGlobalContextInstance());
}

DatabasePtr resolveStatusRequestDatabase(const String & database_name)
{
    return DatabaseCatalog::instance().tryGetDatabase(database_name);
}

}
