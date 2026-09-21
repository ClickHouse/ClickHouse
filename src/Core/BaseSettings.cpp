#include <Core/BaseSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_SETTING;
}


void BaseSettingsHelpers::writeString(std::string_view str, WriteBuffer & out)
{
    writeStringBinary(str, out);
}


String BaseSettingsHelpers::readString(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    return str;
}


void BaseSettingsHelpers::writeFlags(Flags flags, WriteBuffer & out)
{
    writeVarUInt(flags, out);
}


UInt64 BaseSettingsHelpers::readFlags(ReadBuffer & in)
{
    UInt64 res;
    readVarUInt(res, in);
    return res;
}

SettingsTierType BaseSettingsHelpers::getTier(UInt64 flags)
{
    int8_t tier = static_cast<int8_t>(flags & Flags::TIER);
    if (tier > SettingsTierType::BETA)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown tier value: '{}'", tier);
    return static_cast<SettingsTierType>(tier);
}


void BaseSettingsHelpers::throwSettingNotFound(std::string_view name)
{
    throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting '{}'", String{name});
}

/// Log the summary of unknown settings as a warning instead of warning for each one separately.
void BaseSettingsHelpers::warningSettingNotFound(std::string_view name)
{
    unknown_settings.push_back(fmt::format("`{}`", name));

    if (!unknown_settings_warning_logged)
    {
        static size_t MAX_UNKNOWN_SETTINGS_FOR_LOGGING = 3;

        if (unknown_settings.size() > MAX_UNKNOWN_SETTINGS_FOR_LOGGING)
        {
            Strings first_few(unknown_settings.begin(), unknown_settings.begin() + MAX_UNKNOWN_SETTINGS_FOR_LOGGING);
            LOG_WARNING(
                getLogger("Settings"),
                "Unknown settings: {} and {} more, skipping",
                fmt::join(first_few, ", "),
                unknown_settings.size() - MAX_UNKNOWN_SETTINGS_FOR_LOGGING);
        }
        else
        {
            LOG_WARNING(getLogger("Settings"), "Unknown settings: {}, skipping", fmt::join(unknown_settings, ", "));
        }

        unknown_settings_warning_logged = true;
    }
}

void BaseSettingsHelpers::flushWarnings()
{
    unknown_settings.clear();
    unknown_settings_warning_logged = false;
}

}
