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
    /// The engine itself sets the value, so a table does not take it from elsewhere even if it is a setting that others
    /// take from the query or server settings (`StorageFactory::isInheritedSetting`): `value` is the engine's own value
    /// for a table that does not state it, or, with `readonly`, the value the engine always uses.
    bool set_by_engine = false;
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

/// `enumerateCompiledDefaults` without the settings for which `is_unusable` holds: those of `TSettings` that have no effect
/// on a new table of the engine, because the engine rejects them - such as a setting only the database engine of the same
/// name takes, or a deprecated one the engine rejects - or because the engine never reads them, as `Set` and `Hive` do not
/// read the format settings their settings classes declare. A setting that only another engine sharing the settings class reads is
/// not one of them, as the settings only the replicated engines read are not left out of the `MergeTree` rows. They are
/// still rendered before being left out.
template <typename TSettings, bool (*is_unusable)(std::string_view)>
SettingDescriptions enumerateCompiledDefaultsExcept(ContextPtr context)
{
    auto settings = enumerateCompiledDefaults<TSettings>(context);
    std::erase_if(settings, [](const SettingDescription & setting) { return is_unusable(setting.name); });
    return settings;
}

inline bool isHeaderDetectionSetting(std::string_view name)
{
    return name == "input_format_csv_detect_header" || name == "input_format_tsv_detect_header"
        || name == "input_format_custom_detect_header";
}

/// The streaming engines (`Kafka`, `NATS`, `RabbitMQ`, `FileLog`) always disable the header detection of the `CSV`, `TSV`
/// and `CustomSeparated` formats, whatever a table states: these settings show `0`, the value used, as `readonly`.
inline void setHeaderDetectionDisabled(SettingDescriptions & settings)
{
    for (auto & setting : settings)
    {
        if (isHeaderDetectionSetting(setting.name))
        {
            setting.value = "0";
            setting.changed = true;
            setting.readonly = true;
            setting.set_by_engine = true;
        }
    }
}

/// `NATS` and `RabbitMQ`, as `Kafka`, skip unknown fields and allow no ratio of errors when a table does not state these
/// format settings, and allow as many errors as `skip_broken_messages_name` with the default `handle_error_mode` (none with
/// another mode): these settings show the values the engine uses with the default mode, and a table can state others.
inline void setStreamingErrorHandlingDefaults(SettingDescriptions & settings, std::string_view skip_broken_messages_name)
{
    String skip_broken_messages;
    for (const auto & setting : settings)
        if (setting.name == skip_broken_messages_name)
            skip_broken_messages = setting.value;

    for (auto & setting : settings)
    {
        if (setting.name == "input_format_skip_unknown_fields")
            setting.value = "1";
        else if (setting.name == "input_format_allow_errors_ratio")
            setting.value = "0";
        else if (setting.name == "input_format_allow_errors_num")
        {
            setting.value = skip_broken_messages;
            setting.comment += " For a table that does not state it, the value of `" + String(skip_broken_messages_name) + "`.";
        }
        else
            continue;
        setting.changed = true;
        setting.set_by_engine = true;
    }
}

/// `enumerateCompiledDefaults` of a streaming engine, see `setHeaderDetectionDisabled`.
template <typename TSettings>
SettingDescriptions enumerateStreamingEngineDefaults(ContextPtr context)
{
    auto settings = enumerateCompiledDefaults<TSettings>(context);
    setHeaderDetectionDisabled(settings);
    return settings;
}

}
