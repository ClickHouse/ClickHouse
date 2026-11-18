#include <Client/SecondaryQuerySettings.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>


namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool optimize_trivial_group_by_limit_query;
}

void prepareSecondaryQuerySettings(Settings & settings)
{
    settings.markSettingsChangedByCompatibilityAsUnchanged();
    settings[Setting::dialect] = Dialect::clickhouse;

    if (settings[Setting::group_by_overflow_mode].changed && settings[Setting::group_by_overflow_mode] != OverflowMode::ANY)
        settings[Setting::optimize_trivial_group_by_limit_query] = false;
}

}
