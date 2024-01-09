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
        {"default", std::make_shared<DataTypeString>()},
        {"alias_for", std::make_shared<DataTypeString>()},
        {"is_obsolete", std::make_shared<DataTypeUInt8>()},
    };
}

void StorageSystemSettings::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const Settings & settings = context->getSettingsRef();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    const auto fill_data_for_setting = [&](std::string_view setting_name, const auto & setting)
    {
        res_columns[1]->insert(setting.getValueString());
        res_columns[2]->insert(setting.isValueChanged());
        res_columns[3]->insert(setting.getDescription());

        Field min, max;
        SettingConstraintWritability writability = SettingConstraintWritability::WRITABLE;
        constraints.get(settings, setting_name, min, max, writability);

        /// These two columns can accept strings only.
        if (!min.isNull())
            min = Settings::valueToStringUtil(setting_name, min);
        if (!max.isNull())
            max = Settings::valueToStringUtil(setting_name, max);

        res_columns[4]->insert(min);
        res_columns[5]->insert(max);
        res_columns[6]->insert(writability == SettingConstraintWritability::CONST);
        res_columns[7]->insert(setting.getTypeName());
        res_columns[8]->insert(setting.getDefaultValueString());
        res_columns[10]->insert(setting.isObsolete());
    };

    const auto & settings_to_aliases = Settings::Traits::settingsToAliases();
    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        res_columns[0]->insert(setting_name);

        fill_data_for_setting(setting_name, setting);
        res_columns[9]->insert("");

        if (auto it = settings_to_aliases.find(setting_name); it != settings_to_aliases.end())
        {
            for (const auto alias : it->second)
            {
                res_columns[0]->insert(alias);
                fill_data_for_setting(alias, setting);
                res_columns[9]->insert(setting_name);
            }
        }
    }
}

}
