#include <Storages/System/StorageSystemSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>


namespace DB
{
NamesAndTypesList StorageSystemSettings::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeString>()},
        {"changed", std::make_shared<DataTypeUInt8>()},
        {"description", std::make_shared<DataTypeString>()},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"readonly", std::make_shared<DataTypeUInt8>()},
        {"type", std::make_shared<DataTypeString>()},
    };
}

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

void StorageSystemSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const Settings & settings = context->getSettingsRef();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;
    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());

        Field min, max;
        bool read_only = false;
        constraints.get(setting_name, min, max, read_only);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = Settings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = Settings::valueToStringUtil(setting_name, max);

        if (!read_only)
        {
            if ((settings.readonly == 1)
                || ((settings.readonly > 1) && (setting_name == "readonly"))
                || ((!settings.allow_ddl) && (setting_name == "allow_ddl")))
                read_only = true;
        }

        res_columns[4]->insert(min);
        res_columns[5]->insert(max);
        res_columns[6]->insert(read_only);
        res_columns[7]->insert(setting.getTypeName());
    }
}

}
