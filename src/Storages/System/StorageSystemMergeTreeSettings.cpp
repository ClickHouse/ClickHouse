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
#include <Storages/System/SettingsTableColumns.h>


namespace DB
{

template <bool replicated>
ColumnsDescription SystemMergeTreeSettings<replicated>::getColumnsDescription()
{
    return sharedSettingColumns();
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
    auto add_row = [&](std::string_view name, const SettingDescription & setting, std::string_view alias_for)
    {
        /// This table does not override `supportsColumnsMask`, so every column is wanted.
        size_t src_index = 0;
        size_t res_index = 0;
        insertSharedSettingColumns(res_columns, {}, src_index, res_index, name, setting.value, setting, alias_for);
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
