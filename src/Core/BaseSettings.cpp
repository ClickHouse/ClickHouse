#include <Core/BaseSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>


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


void BaseSettingsHelpers::warningSettingNotFound(std::string_view name)
{
    LOG_WARNING(getLogger("Settings"), "Unknown setting '{}', skipping", name);
}

}
