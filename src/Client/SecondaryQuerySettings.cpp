#include <Client/SecondaryQuerySettings.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>


namespace DB
{

namespace Setting
{
    extern const SettingsDialect dialect;
}

void prepareSecondaryQuerySettings(Settings & settings)
{
    settings.markSettingsChangedByCompatibilityAsUnchanged();
    settings[Setting::dialect] = Dialect::clickhouse;
}

}
