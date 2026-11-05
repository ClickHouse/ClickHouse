#include <Client/SecondaryQuerySettings.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>


namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
    extern const SettingsBool enable_fixed_and_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_packed_string_keys_in_aggregation;
}

void prepareSecondaryQuerySettings(Settings & settings)
{
    settings.markSettingsChangedByCompatibilityAsUnchanged();
    settings[Setting::dialect] = Dialect::clickhouse;
}

void markAggregationMethodSettingsChanged(Settings & settings)
{
    settings[Setting::enable_packed_string_keys_in_aggregation]
        = static_cast<bool>(settings[Setting::enable_packed_string_keys_in_aggregation]);
    settings[Setting::enable_fixed_and_packed_string_keys_in_aggregation]
        = static_cast<bool>(settings[Setting::enable_fixed_and_packed_string_keys_in_aggregation]);
}

}
