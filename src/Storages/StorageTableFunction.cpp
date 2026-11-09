#include <Storages/StorageTableFunction.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool data_lake_delete_data_on_drop;
}

void StorageTableFunctionProxy::prepareForDrop(ContextPtr query_context)
{
    /// A requested purge has to reach the data even if the table function has not been resolved yet.
    const bool resolve = can_delete_data_on_drop && query_context->getSettingsRef()[Setting::data_lake_delete_data_on_drop];
    auto storage = resolve ? getNestedImpl() : nestedIfResolved();
    if (storage)
        storage->prepareForDrop(query_context);
}

}
