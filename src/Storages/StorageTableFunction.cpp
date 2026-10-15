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
    auto storage = query_context->getSettingsRef()[Setting::data_lake_delete_data_on_drop] ? getNestedImpl() : nestedIfResolved();
    if (storage)
        storage->prepareForDrop(query_context);
}

}
