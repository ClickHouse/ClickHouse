#include <Client/SecondaryQuerySettings.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <array>


namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
    extern const SettingsBool enable_fixed_and_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_packed_string_keys_in_aggregation;
}

namespace
{

/// The settings that choose the method of an aggregation key, whose hash numbers the two-level buckets.
std::array<SettingsBool, 2> aggregationMethodSettings()
{
    return {Setting::enable_packed_string_keys_in_aggregation, Setting::enable_fixed_and_packed_string_keys_in_aggregation};
}

}

void prepareSecondaryQuerySettings(Settings & settings)
{
    settings.markSettingsChangedByCompatibilityAsUnchanged();
    settings[Setting::dialect] = Dialect::clickhouse;
}

void markAggregationMethodSettingsChanged(Settings & settings)
{
    for (const auto setting : aggregationMethodSettings())
        settings[setting] = static_cast<bool>(settings[setting]);
}

}
