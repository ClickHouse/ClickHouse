#include <Core/BaseSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

namespace DB
{

thread_local Strings BaseSettingsHelpers::unknown_settings;
thread_local bool BaseSettingsHelpers::unknown_settings_warning_logged = false;
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int TYPE_MISMATCH;
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
    UInt64 res = 0;
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

void BaseSettingsHelpers::throwValuelessSettingIsNotBool(std::string_view name, std::string_view type)
{
    throw Exception(
        ErrorCodes::TYPE_MISMATCH,
        "Setting '{}' has type {}, so it cannot be set without a value. Write '{} = <value>'",
        String{name}, String{type}, String{name});
}

void BaseSettingsHelpers::throwValuelessSettingIsNotBool(std::string_view name)
{
    /// For consumers that read a `SettingChange` without a settings schema at hand, so they know the
    /// setting is not Bool but not what its type is.
    throw Exception(
        ErrorCodes::TYPE_MISMATCH,
        "Setting '{}' is not Bool, so it cannot be set without a value. Write '{} = <value>'",
        String{name}, String{name});
}

void BaseSettingsHelpers::throwValuelessSettingHasValue(std::string_view name)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Setting '{}' is marked as written without a value, which stands for `{} = true`, "
        "but it carries a different value", String{name}, String{name});
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
