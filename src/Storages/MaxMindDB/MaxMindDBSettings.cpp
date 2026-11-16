#include <Storages/MaxMindDB/MaxMindDBSettings.h>

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace MaxMindDBSetting
{
extern const MaxMindDBSettingsString refresh_interval;
}

// clang-format off
#define LIST_OF_MAXMINDDB_SETTINGS(DECLARE, ALIAS) \
    DECLARE(String, refresh_interval, "5m", "Interval between checks for a new MMDB generation, using duration syntax such as `5m`, `10m`, or `20h`. `0` disables automatic refresh.", 0) \
    DECLARE(String, disk, "default", "Local disk used to cache remote MMDB files. The disk must support direct access to unencrypted local files.", 0) \
    DECLARE(UInt64, max_download_size, 1073741824, "Maximum download size and total uncompressed archive entry size in bytes. `0` means unlimited.", 0)
// clang-format on

DECLARE_SETTINGS_TRAITS(MaxMindDBSettingsTraits, LIST_OF_MAXMINDDB_SETTINGS, MAXMINDDB_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(MaxMindDBSettingsTraits, LIST_OF_MAXMINDDB_SETTINGS, MaxMindDBSettings, MaxMindDBSetting)

MaxMindDBSettings::MaxMindDBSettings()
    : impl(std::make_unique<MaxMindDBSettingsImpl>())
{
}
MaxMindDBSettings::~MaxMindDBSettings() = default;

MAXMINDDB_SETTINGS_SUPPORTED_TYPES(MaxMindDBSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void MaxMindDBSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
        impl->applyChanges(storage_def.settings->changes);
}

bool MaxMindDBSettings::hasBuiltin(std::string_view name)
{
    return MaxMindDBSettingsImpl::hasBuiltin(name);
}

UInt64 MaxMindDBSettings::refreshIntervalMilliseconds() const
{
    auto milliseconds = parseTimeSeriesDuration((*this)[MaxMindDBSetting::refresh_interval].value, 3).value;
    if (milliseconds < 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB refresh_interval must be nonnegative");
    return milliseconds;
}
}
