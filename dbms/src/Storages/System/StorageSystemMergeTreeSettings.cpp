#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>


namespace DB
{

NamesAndTypesList SystemMergeTreeSettings::getNamesAndTypes()
{
    return {
        {"name",    std::make_shared<DataTypeString>()},
        {"value",   std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
    };
}

void SystemMergeTreeSettings::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const MergeTreeSettings & settings = context.getMergeTreeSettings();

#define ADD_SETTING(TYPE, NAME, DEFAULT)              \
    res_columns[0]->insert(String(#NAME));            \
    res_columns[1]->insert(settings.NAME.toString()); \
    res_columns[2]->insert(UInt64(settings.NAME.changed));
    APPLY_FOR_MERGE_TREE_SETTINGS(ADD_SETTING)
#undef ADD_SETTING
}

}
