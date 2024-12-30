#include <Storages/System/StorageSystemSettings.h>

#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Core/Settings.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/MutableColumnsAndConstraints.h>


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
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
    };
}

void StorageSystemSettings::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const Settings & settings = context->getSettingsRef();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    MutableColumnsAndConstraints params(res_columns, constraints);
    settings.dumpToSystemSettingsColumns(params);
}

}
