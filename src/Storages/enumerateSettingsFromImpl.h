#pragma once

#include <Storages/SettingDescription.h>
#include <Core/BaseSettings.h>
#include <Storages/maskEngineSettingValue.h>

namespace DB
{

/// Reads every setting of a settings object whose traits record origins into the form both settings tables use. A
/// changed setting reports the origin the object recorded, else `Other`, which a storage's override may refine.
template <typename TTraits>
SettingDescriptions enumerateSettingsFromImpl(const BaseSettings<TTraits> & impl)
    requires TTraits::record_origin
{
    const auto & settings_to_aliases = TTraits::settingsToAliases();

    SettingDescriptions result;
    for (const auto & setting : impl.all())
    {
        SettingDescription described;
        described.name = setting.getName();
        /// The real value, which `masked_value` hides from a reader who may not see it. A built-in secret has an empty
        /// default, as `05214_engine_settings_secrets_have_empty_default` asserts.
        described.value = setting.getValueString(/* show_secrets */ true);
        described.default_value = setting.getDefaultValueString(/* show_secrets */ false);
        described.type = setting.getTypeName();
        described.comment = setting.getDescription();
        described.tier = setting.getTier();

        if (const auto it = settings_to_aliases.find(described.name); it != settings_to_aliases.end())
            described.aliases.assign(it->second.begin(), it->second.end());

        /// While the `Field` is still here: a setting whose value is an AST cannot be masked from
        /// the rendered string alone. A value equal to the compiled-in default holds no credential, and
        /// masking it would make an unset password claim to hide one.
        if (described.value != described.default_value)
            described.masked_value = maskEngineSettingValue(described, setting.getValue());
        described.origin = setting.isValueChanged() ? SettingOrigin::Other : SettingOrigin::Default;

        /// A recorded value that merely equals the default is still changed, and keeps its source.
        if (described.origin == SettingOrigin::Other)
            if (const auto recorded = impl.recordedOrigin(described.name); recorded != SettingOrigin::Default)
                described.origin = recorded;
        if (described.origin == SettingOrigin::NamedCollection)
            described.named_collection = impl.recordedNamedCollection();
        result.push_back(std::move(described));
    }
    return result;
}

/// Defines `TYPE::enumerateSettings`. Belongs in the settings struct's .cpp, the only place its `Impl` type is
/// complete.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    SettingDescriptions TYPE::enumerateSettings() const { return enumerateSettingsFromImpl(*impl); }

}
