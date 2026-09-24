#pragma once

#include <Storages/SettingDescription.h>
#include <Storages/SettingsWithRecordedOrigin.h>
#include <Storages/maskEngineSettingValue.h>

namespace DB
{

/// Reads every setting of a `SettingsWithRecordedOrigin` instance into the common form both settings tables use.
///
/// The instance decides what is reported: a default-constructed one describes an engine, the one a
/// storage holds describes a table. `origin` is the source the instance recorded, and otherwise `Other` for
/// a changed setting: the instance cannot tell the rest apart, so a storage refines it, since only the
/// storage knows where the rest of its values came from.
template <typename TTraits>
SettingDescriptions enumerateSettingsFromImpl(const SettingsWithRecordedOrigin<TTraits> & impl)
{
    const auto & settings_to_aliases = TTraits::settingsToAliases();

    SettingDescriptions result;
    for (const auto & setting : impl.all())
    {
        SettingDescription described;
        described.name = setting.getName();
        /// The real value, which `masked_value` below hides when the reader may not see it. The flag on the
        /// default is defence in depth and nothing more: `getDefaultValueString` honours it for a custom
        /// setting only, and a built-in one holding a secret has an empty default anyway, which
        /// `05214_engine_settings_secrets_have_empty_default` asserts for every engine.
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

        /// A recorded value that merely equals the default is still changed, and keeps its source.
        if (described.origin == SettingOrigin::Other)
            if (const auto recorded = impl.recordedOrigin(described.name); recorded != SettingOrigin::Default)
                described.origin = recorded;
        if (described.origin == SettingOrigin::NamedCollection)
            described.named_collection = impl.recordedNamedCollection();
        if (const auto it = settings_to_aliases.find(described.name); it != settings_to_aliases.end())
            described.aliases.assign(it->second.begin(), it->second.end());
        result.push_back(std::move(described));
    }
    return result;
}

/// Defines `TYPE::enumerateSettings`. Belongs in the settings struct's .cpp, the only place its `Impl` type is
/// complete.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    SettingDescriptions TYPE::enumerateSettings() const { return enumerateSettingsFromImpl(*impl); }

/// Defines `TYPE::nameAtOffset`, for a settings class whose engine names one of its settings by typed index.
/// Belongs in the same .cpp, for the same reason.
#define IMPLEMENT_SETTINGS_NAME_AT_OFFSET(TYPE) \
    std::string_view TYPE::nameAtOffset(size_t offset) { return TYPE##Impl::nameAtOffset(offset); }

}
