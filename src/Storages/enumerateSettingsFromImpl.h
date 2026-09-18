#pragma once

#include <Storages/SettingDescription.h>
#include <Storages/maskEngineSettingValue.h>

namespace DB
{

/// Reads every setting of a `BaseSettings` instance into the common form both settings tables use.
///
/// The instance decides what is reported: a default-constructed one describes an engine, the one a
/// storage holds describes a table. `origin` is only as precise as the instance allows: the source an
/// instance built on `SettingsWithRecordedOrigin` recorded, and otherwise `Other` for a changed setting,
/// because a plain instance cannot tell a config section from a named collection. A storage refines what
/// is left, since only the storage knows where the rest of its values came from.
template <typename SettingsImplType>
SettingDescriptions enumerateSettingsFromImpl(const SettingsImplType & impl)
{
    const auto & settings_to_aliases = SettingsImplType::Traits::settingsToAliases();

    SettingDescriptions result;
    for (const auto & setting : impl.all())
    {
        SettingDescription described;
        described.name = setting.getName();
        /// The real value, which `masked_value` below hides when the reader may not see it. A
        /// default is compiled in and holds no credential, so it needs no such treatment.
        described.value = setting.getValueString(/* show_secrets */ true);
        described.default_value = setting.getDefaultValueString(/* show_secrets */ false);
        described.type = setting.getTypeName();
        described.comment = setting.getDescription();
        described.tier = setting.getTier();

        /// While the `Field` is still here: a setting whose value is an AST cannot be masked from
        /// the rendered string alone. A value equal to the compiled-in default holds no credential, and
        /// masking it would make an unset password claim to hide one.
        if (described.value != described.default_value)
            described.masked_value = maskEngineSettingValue(described.name, setting.getValue(), described.value);
        described.origin = setting.isValueChanged() ? SettingOrigin::Other : SettingOrigin::Default;
        if constexpr (requires { impl.recordedOrigin(described.name); })
            if (const auto recorded = impl.recordedOrigin(described.name))
                described.origin = *recorded;
        if (const auto it = settings_to_aliases.find(described.name); it != settings_to_aliases.end())
            described.aliases.assign(it->second.begin(), it->second.end());
        result.push_back(std::move(described));
    }
    return result;
}

/// Defines what `DECLARE_SETTINGS_ENUMERATION` declares. Belongs in the settings struct's .cpp,
/// the only place its `Impl` type is complete.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    SettingDescriptions TYPE::enumerateSettings() const { return enumerateSettingsFromImpl(*impl); }

}
