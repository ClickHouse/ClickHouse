#pragma once

#include <Common/Exception.h>

#include <Disks/ObjectStorages/VFS/VFSLog.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <variant>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int CORRUPTED_DATA;
}

class JSONSerializer
{
public:
    static std::vector<char> serialize(const VFSEvent & event)
    {
        Poco::JSON::Object object;

        serializeToJSON(event, object);
        String str = to_string(object);

        return {str.begin(), str.end()};
    }

    static VFSEvent deserialize(std::span<char> buffer)
    {
        String str(buffer.data(), buffer.size());
        auto object = from_string(std::move(str));

        if (!object)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot parse VFS log item buffer as JSON");

        VFSEvent event;
        deserializeFromJSON(event, *object);

        return event;
    }

private:
    template <typename Value>
    static Value getOrThrow(const Poco::JSON::Object & object, const String & key)
    {
        if (!object.has(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Key {} is not found in VFS log item", key);
        return object.getValue<Value>(key);
    }

    static String to_string(const Poco::JSON::Object & object)
    {
        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(object, oss);

        return oss.str();
    }

    static Poco::JSON::Object::Ptr from_string(const String & json_str)
    {
        Poco::JSON::Parser parser;
        return parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();
    }

    static void serializeToJSON(const VFSEvent & event, Poco::JSON::Object & object)
    {
        object.set("remote_path", event.remote_path);
        object.set("local_path", event.local_path);
        object.set("action", static_cast<std::underlying_type_t<VFSAction>>(event.action));
        object.set("timestamp", event.timestamp.epochMicroseconds());

        if (event.orig_wal.has_value())
        {
            Poco::JSON::Object orig_wal;
            serializeToJSON(*event.orig_wal, orig_wal);

            object.set("orig_wal", orig_wal);
        }
    }

    static void serializeToJSON(const WALInfo & orig_wal, Poco::JSON::Object & object)
    {
        object.set("index", orig_wal.index);
        object.set("id", toString(orig_wal.id));
        object.set("replica", orig_wal.replica);
    }

    static void deserializeFromJSON(VFSEvent & event, const Poco::JSON::Object & json)
    {
        event.remote_path = getOrThrow<String>(json, "remote_path");
        event.local_path = getOrThrow<String>(json, "local_path");
        event.action = static_cast<VFSAction>(getOrThrow<std::underlying_type_t<VFSAction>>(json, "action"));
        event.timestamp = getOrThrow<UInt64>(json, "timestamp");

        if (auto orig_wal = json.getObject("orig_wal"); orig_wal)
            deserializeFromJSON(event.orig_wal.emplace(), *orig_wal);
    }

    static void deserializeFromJSON(WALInfo & orig_wal, const Poco::JSON::Object & json)
    {
        orig_wal.id = parseFromString<UUID>(getOrThrow<String>(json, "id"));
        orig_wal.index = getOrThrow<UInt64>(json, "index");
        orig_wal.replica = getOrThrow<String>(json, "replica");
    }
};

}
