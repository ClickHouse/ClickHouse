#pragma once

#include <Core/BaseSettings.h>
#include <Storages/SettingDescription.h>
#include <Common/SettingsChanges.h>

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

/// Applies the changes whose names `settings` declares. Other names are not settings of this object - a table's
/// `SETTINGS` clause can also carry, for example, format settings that the engine handles elsewhere.
template <typename TSettingsImpl>
void applyDeclaredChanges(TSettingsImpl & settings, const SettingsChanges & changes)
{
    for (const auto & change : changes)
        if (TSettingsImpl::hasBuiltin(change.name))
            settings.applyChange(change);
}

/// Defines `TYPE::enumerateSettings`. Belongs in the settings struct's .cpp, the only place its `Impl` type is
/// complete. The changes are applied to a copy; the object itself is not modified.
#define IMPLEMENT_SETTINGS_ENUMERATION(TYPE) \
    SettingDescriptions TYPE::enumerateSettings(const SettingsChanges & changes) const \
    { \
        if (changes.empty()) \
            return enumerateSettingsFromImpl(*impl); \
        auto settings = *impl; \
        applyDeclaredChanges(settings, changes); \
        return enumerateSettingsFromImpl(settings); \
    }

}
