#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>


namespace DB
{

template <bool replicated>
NamesAndTypesList SystemMergeTreeSettings<replicated>::getNamesAndTypes()
{
    return {
        {"name",        std::make_shared<DataTypeString>()},
        {"value",       std::make_shared<DataTypeString>()},
        {"changed",     std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"type",        std::make_shared<DataTypeString>()},
    };
}

template <bool replicated>
void SystemMergeTreeSettings<replicated>::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto & settings = replicated ? context.getReplicatedMergeTreeSettings().all() : context.getMergeTreeSettings().all();
    for (const auto & setting : settings)
    {
        res_columns[0]->insert(setting.getName());
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());
        res_columns[4]->insert(setting.getTypeName());
    }
}

template class SystemMergeTreeSettings<false>;
template class SystemMergeTreeSettings<true>;
}
