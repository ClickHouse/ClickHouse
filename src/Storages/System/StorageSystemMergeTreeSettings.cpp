#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>


namespace DB
{

template <bool replicated>
ColumnsDescription SystemMergeTreeSettings<replicated>::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name",        std::make_shared<DataTypeString>(), "Setting name."},
        {"value",       std::make_shared<DataTypeString>(), "Setting value."},
        {"default",     std::make_shared<DataTypeString>(), "Setting default value."},
        {"changed",     std::make_shared<DataTypeUInt8>(), "1 if the setting was explicitly defined in the config or explicitly changed."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"min",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Minimum value of the setting, if any is set via constraints. If the setting has no minimum value, contains NULL."},
        {"max",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Maximum value of the setting, if any is set via constraints. If the setting has no maximum value, contains NULL."},
        {"disallowed_values",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "List of disallowed values"},
        {"readonly",    std::make_shared<DataTypeUInt8>(),
            "Shows whether the current user can change the setting: "
            "0 — Current user can change the setting, "
            "1 — Current user can't change the setting."
        },
        {"type",        std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* PRIVATE PREVIEW: The feature is on a clear path to general availability. Its applicability is still limited and it is not recommended for production use.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
        {"alias_for",   std::make_shared<DataTypeString>(),
            "Empty on a setting's own row. A setting writable under more than one name also gets a row per other name, "
            "carrying the same values, with this naming the one it is declared under. As in `system.settings`."},
    };
}

template <bool replicated>
void SystemMergeTreeSettings<replicated>::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    /// The same enumeration `system.engine_settings` reads, so the two tables cannot drift apart -
    /// this one is that one restricted to a single engine family. It carries the settings
    /// constraints of the current user already, which is what `dumpToSystemMergeTreeSettingsColumns`
    /// used to do here.
    const auto settings = replicated
        ? MergeTreeSettings::enumerateReplicatedEngineSettings(context)
        : MergeTreeSettings::enumerateEngineSettings(context);

    /// A row per name the setting answers to, as `system.settings` does.
    auto add_row = [&](std::string_view name, const TableSetting & setting, std::string_view alias_for)
    {
        size_t i = 0;
        res_columns[i++]->insert(name);
        res_columns[i++]->insert(setting.value);
        res_columns[i++]->insert(setting.default_value);
        res_columns[i++]->insert(setting.origin != TableSettingOrigin::Default);
        res_columns[i++]->insert(setting.description);
        res_columns[i++]->insert(setting.min_value ? Field(*setting.min_value) : Field());
        res_columns[i++]->insert(setting.max_value ? Field(*setting.max_value) : Field());

        Array disallowed;
        disallowed.reserve(setting.disallowed_values.size());
        for (const auto & disallowed_value : setting.disallowed_values)
            disallowed.emplace_back(disallowed_value);
        res_columns[i++]->insert(disallowed);

        res_columns[i++]->insert(setting.readonly);
        res_columns[i++]->insert(setting.type);
        res_columns[i++]->insert(setting.tier == SettingsTierType::OBSOLETE);
        res_columns[i++]->insert(setting.tier);
        res_columns[i++]->insert(alias_for);
    };

    for (const auto & setting : settings)
    {
        add_row(setting.name, setting, "");
        for (const auto alias : setting.aliases)
            add_row(alias, setting, setting.name);
    }
}

template class SystemMergeTreeSettings<false>;
template class SystemMergeTreeSettings<true>;
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(SystemMergeTreeSettings<false>) }
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(SystemMergeTreeSettings<true>) }
