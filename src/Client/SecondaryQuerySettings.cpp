#include <Client/SecondaryQuerySettings.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <algorithm>
#include <array>


namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
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
    settings.set(Setting::dialect, Dialect::clickhouse);
}

void markAggregationMethodSettingsChanged(Settings & settings)
{
    for (const auto setting : aggregationMethodSettings())
        settings.set(setting, static_cast<bool>(settings[setting]));
}

bool aggregationMethodSettingsDifferFromInitiator(const Settings & initiator_settings, const Settings & settings)
{
    return std::ranges::any_of(aggregationMethodSettings(), [&](const auto setting)
    {
        return initiator_settings[setting].changed && initiator_settings[setting].value != settings[setting].value;
    });
}

bool explicitGroupByOverflowModeIsLost(const Settings & initiator_settings, const Settings & settings)
{
    return initiator_settings[Setting::group_by_overflow_mode].changed && !settings[Setting::group_by_overflow_mode].changed
        && initiator_settings[Setting::group_by_overflow_mode].value == settings[Setting::group_by_overflow_mode].value;
}

}
