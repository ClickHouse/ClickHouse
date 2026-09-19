#include <Storages/System/SystemTableSourceRegistry.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Storages/System/SettingsTableColumns.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>


namespace DB
{

template <bool replicated>
ColumnsDescription SystemMergeTreeSettings<replicated>::getColumnsDescription()
{
    return sharedSettingColumns();
}

template <bool replicated>
void SystemMergeTreeSettings<replicated>::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const
{
    /// The same enumeration `system.engine_settings` reads, so the two tables cannot drift apart -
    /// this one is that one restricted to a single engine family, with the current user's settings
    /// constraints already applied.
    const auto settings = replicated
        ? MergeTreeSettings::enumerateReplicatedEngineSettings(context)
        : MergeTreeSettings::enumerateEngineSettings(context);

    const bool show_secrets = canDisplaySecrets(context);
    SettingRowWriter writer(res_columns, columns_mask);
    for (const auto & setting : settings)
        writeSettingRows(
            writer, setting, isSettingValueMasked(setting, show_secrets), [](SettingRowWriter &) {}, [](SettingRowWriter &) {});
}

template class SystemMergeTreeSettings<false>;
template class SystemMergeTreeSettings<true>;
}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(SystemMergeTreeSettings<false>) }
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(SystemMergeTreeSettings<true>) }
