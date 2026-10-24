#pragma once

#include <Core/BaseSettings.h>
#include <Storages/SettingDescription.h>

namespace DB
{

/// Describes every setting of a settings object.
template <typename TTraits>
SettingDescriptions enumerateSettingsFromImpl(const BaseSettings<TTraits> & impl)
{
    SettingDescriptions result;
    for (const auto & setting : impl.all())
    {
        SettingDescription described;
        described.name = setting.getName();
        described.value = setting.getValueString(/* show_secrets */ true);
        described.default_value = setting.getDefaultValueString(/* show_secrets */ true);
        described.changed = setting.isValueChanged();
        described.type = setting.getTypeName();
        described.comment = setting.getDescription();
        described.tier = setting.getTier();
        result.push_back(std::move(described));
    }
    return result;
}

/// Defines `TYPE::enumerateSettings`. Belongs in the settings struct's .cpp, the only place its `Impl` type is
/// complete.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    SettingDescriptions TYPE::enumerateSettings() const { return enumerateSettingsFromImpl(*impl); }

}
