#pragma once

#include <Core/SettingsTierType.h>
#include <base/types.h>

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
};

using SettingDescriptions = std::vector<SettingDescription>;

/// The compiled defaults of `TSettings`. Registered as `.enumerate_engine_settings_fn = enumerateCompiledDefaults<TSettings>`.
///
/// `TSettings::enumerateSettings` describes one instance; every settings struct declares it and defines it with
/// `IMPLEMENT_SETTINGS_ENUMERATION`.
template <typename TSettings>
SettingDescriptions enumerateCompiledDefaults()
{
    return TSettings{}.enumerateSettings();
}

}
