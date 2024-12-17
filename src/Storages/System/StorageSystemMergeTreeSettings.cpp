#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>


namespace DB
{

template <bool replicated>
ColumnsDescription SystemMergeTreeSettings<replicated>::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name",        std::make_shared<DataTypeString>(), "Setting name."},
        {"value",       std::make_shared<DataTypeString>(), "Setting value."},
        {"changed",     std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"min",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."},
        {"max",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."},
        {"readonly",    std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
        {"type",        std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
    };
}

template <bool replicated>
void SystemMergeTreeSettings<replicated>::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & settings = replicated ? context->getReplicatedMergeTreeSettings() : context->getMergeTreeSettings();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    MutableColumnsAndConstraints params(res_columns, constraints);
    settings.dumpToSystemMergeTreeSettingsColumns(params);
}

template class SystemMergeTreeSettings<false>;
template class SystemMergeTreeSettings<true>;
}
