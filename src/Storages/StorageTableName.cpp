#include <Storages/StorageTableName.h>
#include <Interpreters/Context.h>

namespace DB
{

StoragePtr StorageTableName::getNested() const
{
    std::lock_guard lock{nested_mutex};
    if (nested)
        return nested;

    auto nested_storage = DatabaseCatalog::instance().getTable(nested_table_id, global_context);
    nested_storage->startup();
    nested_storage->renameInMemory(getStorageID());
    nested = nested_storage;
    return nested;
}

}
