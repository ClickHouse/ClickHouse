#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>


namespace DB
{

NamesAndTypesList SystemMergeTreeSettings::getNamesAndTypes()
{
    return {
        {"name",        std::make_shared<DataTypeString>()},
        {"value",       std::make_shared<DataTypeString>()},
        {"changed",     std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"type",        std::make_shared<DataTypeString>()},
    };
}

void SystemMergeTreeSettings::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    for (auto setting : context.getMergeTreeSettings().all())
    {
        res_columns[0]->insert(setting.getName());
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());
        res_columns[4]->insert(setting.getTypeName());
    }
}

}
