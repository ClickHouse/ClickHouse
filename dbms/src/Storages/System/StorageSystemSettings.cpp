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
    const Settings & settings = context.getSettingsRef();

#define ADD_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION)      \
    res_columns[0]->insert(String(#NAME));                 \
    res_columns[1]->insert(settings.NAME.toString());      \
    res_columns[2]->insert(UInt64(settings.NAME.changed)); \
    res_columns[3]->insert(String(DESCRIPTION));
    APPLY_FOR_SETTINGS(ADD_SETTING)
#undef ADD_SETTING
}

}
