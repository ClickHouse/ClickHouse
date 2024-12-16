#include <Storages/System/StorageSystemSettings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>


namespace DB
{
ColumnsDescription StorageSystemSettings::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Setting name."},
        {"value", std::make_shared<DataTypeString>(), "Setting value."},
        {"changed", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is changed from its default value."},
        {"description", std::make_shared<DataTypeString>(), "Short setting description."},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."
        },
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."
        },
        {"readonly", std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
        {"type", std::make_shared<DataTypeString>(), "The type of the value that can be assigned to this setting."},
        {"default", std::make_shared<DataTypeString>(), "Setting default value."},
        {"alias_for", std::make_shared<DataTypeString>(), "Flag that shows whether this name is an alias to another setting."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
    };
}

void StorageSystemSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
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
