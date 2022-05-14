#include <Core/BaseSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

void BaseSettingsHelpers::writeString(const std::string_view & str, WriteBuffer & out)
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


BaseSettingsHelpers::Flags BaseSettingsHelpers::readFlags(ReadBuffer & in)
{
    UInt64 res;
    readVarUInt(res, in);
    return static_cast<Flags>(res);
}


void BaseSettingsHelpers::throwSettingNotFound(const std::string_view & name)
{
    throw Exception("Unknown setting " + String{name}, ErrorCodes::UNKNOWN_SETTING);
}


void BaseSettingsHelpers::warningSettingNotFound(const std::string_view & name)
{
    static auto * log = &Poco::Logger::get("Settings");
    LOG_WARNING(log, "Unknown setting {}, skipping", name);
}

}
