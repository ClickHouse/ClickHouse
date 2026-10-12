#pragma once

#include <Core/SettingsTierType.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

/// One setting of a table engine, as `system.engine_settings` shows it.
struct SettingDescription
{
    String name;
    String value;
    String default_value;
    bool changed = false;
    String type;
    String comment;
    SettingsTierType tier = SettingsTierType::PRODUCTION;
    /// From the current user's settings constraints, where any apply to the setting. `readonly` is also set for a
    /// setting the engine never lets change (`MergeTreeSettings::isReadonlySetting`).
    std::optional<String> min_value;
    std::optional<String> max_value;
    std::vector<String> disallowed_values;
    bool readonly = false;
};

using SettingDescriptions = std::vector<SettingDescription>;

/// The compiled defaults of `TSettings`, which is what a new table of an engine without a server-level settings
/// instance starts from. Registered as `.enumerate_engine_settings_fn = enumerateCompiledDefaults<TSettings>`.
///
/// `TSettings::enumerateSettings` describes one instance; every settings struct declares it and defines it with
/// `IMPLEMENT_SETTINGS_ENUMERATION`.
template <typename TSettings>
SettingDescriptions enumerateCompiledDefaults(ContextPtr)
{
    return TSettings{}.enumerateSettings();
}

/// `enumerateCompiledDefaults` without the settings for which `is_unusable` holds: those of `TSettings` that a new table
/// of the engine cannot use, because the engine rejects them or replaces their value with its own - such as a setting
/// only the database engine of the same name takes, or a deprecated one the engine rejects. A setting the engine accepts
/// but does not read is not one of them, as the settings only the replicated engines read are not left out of the
/// `MergeTree` rows. They are still rendered before being left out.
template <typename TSettings, bool (*is_unusable)(std::string_view)>
SettingDescriptions enumerateCompiledDefaultsExcept(ContextPtr context)
{
    auto settings = enumerateCompiledDefaults<TSettings>(context);
    std::erase_if(settings, [](const SettingDescription & setting) { return is_unusable(setting.name); });
    return settings;
}

}
