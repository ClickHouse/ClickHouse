#pragma once

#include <Storages/TableSetting.h>
#include <Storages/maskEngineSettingValue.h>

namespace DB
{

/// Reads every setting of a `BaseSettings` instance into the common form both settings tables use.
///
/// The instance decides what is reported: a default-constructed one describes an engine, the one a
/// storage holds describes a table. `origin` is only as precise as the instance allows - a setting
/// that differs from its default is `Other` here, because this cannot tell a config section from a
/// named collection. A storage refines it, since only the storage knows where its values came from.
template <typename SettingsImplType>
TableSettings enumerateSettingsFromImpl(const SettingsImplType & impl)
{
    const auto & settings_to_aliases = SettingsImplType::Traits::settingsToAliases();

    TableSettings result;
    for (const auto & setting : impl.all())
    {
        TableSetting described;
        described.name = setting.getName();
        /// The real value, which `masked_value` below hides when the reader may not see it. A
        /// default is compiled in and holds no credential, so it needs no such treatment.
        described.value = setting.getValueString(/* show_secrets */ true);
        described.default_value = setting.getDefaultValueString(/* show_secrets */ false);
        described.type = setting.getTypeName();
        described.description = setting.getDescription();
        described.tier = setting.getTier();

        /// While the `Field` is still here: a setting whose value is an AST cannot be masked from
        /// the rendered string alone.
        String masked = described.value;
        if (maskEngineSettingValue(described.name, setting.getValue(), masked))
            described.masked_value = std::move(masked);
        described.origin = setting.isValueChanged() ? TableSettingOrigin::Other : TableSettingOrigin::Default;
        if (const auto it = settings_to_aliases.find(described.name); it != settings_to_aliases.end())
            described.aliases.assign(it->second.begin(), it->second.end());
        result.push_back(std::move(described));
    }
    return result;
}

/// Defines what `DECLARE_SETTINGS_ENUMERATION` declares. Belongs in the settings struct's .cpp,
/// the only place its `Impl` type is complete.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    TableSettings TYPE::enumerateSettings() const { return enumerateSettingsFromImpl(*impl); }

}
