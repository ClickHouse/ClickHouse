#pragma once

#include <Core/SettingsTierType.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <optional>
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
    /// From the current user's settings constraints, where any apply to the setting.
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

}
