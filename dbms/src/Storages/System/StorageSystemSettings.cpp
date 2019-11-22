#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemSettings.h>


namespace DB
{

NamesAndTypesList StorageSystemSettings::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
    };
}

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

void StorageSystemSettings::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    for (const auto & setting : context.getSettingsRef())
    {
        res_columns[0]->insert(setting.getName().toString());
        res_columns[1]->insert(setting.getValueAsString());
        res_columns[2]->insert(setting.isChanged());
        res_columns[3]->insert(setting.getDescription().toString());
    }
}

}
