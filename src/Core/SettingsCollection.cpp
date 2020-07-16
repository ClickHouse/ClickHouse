#include <Core/SettingsCollection.h>
#include <Core/SettingsCollectionImpl.h>

#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <common/logger_useful.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}


namespace details
{
    void SettingsCollectionUtils::serializeName(const StringRef & name, WriteBuffer & buf)
    {
        writeStringBinary(name, buf);
    }

    String SettingsCollectionUtils::deserializeName(ReadBuffer & buf)
    {
        String name;
        readStringBinary(name, buf);
        return name;
    }

    void SettingsCollectionUtils::serializeFlag(bool flag, WriteBuffer & buf)
    {
        buf.write(flag);
    }

    bool SettingsCollectionUtils::deserializeFlag(ReadBuffer & buf)
    {
        char c;
        buf.readStrict(c);
        return c;
    }

    void SettingsCollectionUtils::skipValue(ReadBuffer & buf)
    {
        /// Ignore a string written by the function writeStringBinary().
        UInt64 size;
        readVarUInt(size, buf);
        buf.ignore(size);
    }

    void SettingsCollectionUtils::warningNameNotFound(const StringRef & name)
    {
        static auto * log = &Poco::Logger::get("Settings");
        LOG_WARNING(log, "Unknown setting {}, skipping", name);
    }

    void SettingsCollectionUtils::throwNameNotFound(const StringRef & name)
    {
        throw Exception("Unknown setting " + name.toString(), ErrorCodes::UNKNOWN_SETTING);
    }
}
}
